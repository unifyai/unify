import express, { Request, Response } from 'express';
import https from 'https';
import http from 'http';
import expressWs from 'express-ws';
import WebSocket from 'ws';
import util from 'util';
import { startBrowserAgent, BrowserAgent, BrowserConnector, AgentError, BrowserOptions, AgentMemory, Observation, Image, formatLastBrowserLifecycleHint } from 'magnitude-core';
import { z, ZodTypeAny, ZodAny, ZodType } from 'zod';
import { partitionHtml, serializeToMarkdown, PartitionOptions, MarkdownSerializerOptions } from 'magnitude-extract';
import dotenv from 'dotenv';
import { EgressPolicyError, parseEgressPolicy, resolveEgress, type ResolvedEgress } from './egressPolicy';
dotenv.config();
import os from 'os';
import path from 'path';
import fs from 'fs';
import net from 'net';
import { randomUUID } from 'crypto';
import { ChildProcess, spawn, execSync } from 'child_process';
import { registerExec, signalExec } from './execControl';
import multer from 'multer';
import { jsonSchemaToZod } from './jsonSchemaToZod';
import { getLlmConfig, resolveAgentServiceModel } from './llmConfig';
import {
  computeNativeObservationScale,
  NativeObservationScale,
  ObservationScalingPolicy,
  resolveObservationScalingPolicy,
  scaleObservationCoordsToDisplay,
} from './observationScaling';

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

const DESKTOP_NOVNC_HOST = process.env.DESKTOP_NOVNC_HOST || '127.0.0.1';
const DESKTOP_NOVNC_PORT = Number(process.env.DESKTOP_NOVNC_PORT || 6080);

/** VNC password is the per-binding desktop secret; falls back to setup.sh's convention (first 8 chars of UNIFY_KEY). */
function buildDesktopNoVncUrl(): string {
  const password = process.env.VNC_PASSWORD || (process.env.UNIFY_KEY || '').slice(0, 8);
  const params = new URLSearchParams({
    password,
    autoconnect: '1',
    resize: 'scale',
    reconnect: '1',
    show_dot: '1',
  });
  return `http://${DESKTOP_NOVNC_HOST}:${DESKTOP_NOVNC_PORT}/vnc.html?${params}`;
}

/** Playwright desktop mode loads the local noVNC page; fail fast if websockify is down. */
async function waitForLocalNoVnc(timeoutMs = 20000): Promise<void> {
  const probeUrl = buildDesktopNoVncUrl();
  const deadline = Date.now() + timeoutMs;
  let lastError = 'unknown';
  while (Date.now() < deadline) {
    try {
      const resp = await fetch(probeUrl, { redirect: 'manual' });
      if (resp.ok) {
        return;
      }
      lastError = `HTTP ${resp.status}`;
    } catch (err) {
      lastError = err instanceof Error ? err.message : String(err);
    }
    await sleep(500);
  }
  throw new Error(
    `noVNC is not reachable at http://${DESKTOP_NOVNC_HOST}:${DESKTOP_NOVNC_PORT} (${lastError}). ` +
    'Start websockify on port 6080 before user-desktop control.',
  );
}

// --- Debug logging helpers ---
const MAGNITUDE_DEBUG = process.env.MAGNITUDE_DEBUG === 'true';
const MAGNITUDE_LOG_DIR = process.env.MAGNITUDE_LOG_DIR || '';

function makeActId(task: string): string {
  const ts = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
  const slug = task.slice(0, 40).replace(/[^a-zA-Z0-9]+/g, '_').replace(/_+$/, '');
  return `${ts}_${slug}`;
}

function debugSaveImage(actId: string, label: string, base64Data: string): void {
  if (!MAGNITUDE_DEBUG || !MAGNITUDE_LOG_DIR) return;
  try {
    const imgPath = path.join(MAGNITUDE_LOG_DIR, 'acts', actId, `${label}.png`);
    fs.mkdirSync(path.dirname(imgPath), { recursive: true });
    fs.writeFileSync(imgPath, Buffer.from(base64Data, 'base64'));
  } catch (err) {
    console.warn(`[debug] Failed to save image ${label}: ${err}`);
  }
}

function debugSaveTrace(actId: string, trace: Record<string, any>): void {
  if (!MAGNITUDE_DEBUG || !MAGNITUDE_LOG_DIR) return;
  try {
    const tracePath = path.join(MAGNITUDE_LOG_DIR, 'acts', actId, 'act_trace.json');
    fs.mkdirSync(path.dirname(tracePath), { recursive: true });
    fs.writeFileSync(tracePath, JSON.stringify(trace, null, 2));
  } catch (err) {
    console.warn(`[debug] Failed to save trace: ${err}`);
  }
}

function debugLog(line: string): void {
  if (!MAGNITUDE_DEBUG || !MAGNITUDE_LOG_DIR) return;
  try {
    fs.mkdirSync(MAGNITUDE_LOG_DIR, { recursive: true });
    fs.appendFileSync(path.join(MAGNITUDE_LOG_DIR, 'magnitude.log'), line + '\n');
  } catch (_) { /* best-effort */ }
}

// --- File System and Command Execution Utilities ---
//
// Workspace root for file operations, command execution, and browser downloads.
// Matches Unity's get_local_root() default of ~/Unity/Local.
// Override via UNITY_LOCAL_ROOT env var.
const LOCAL_ROOT = process.env.UNITY_LOCAL_ROOT || path.join(os.homedir(), 'Unity', 'Local');
try { fs.mkdirSync(LOCAL_ROOT, { recursive: true }); } catch (_e) { /* ignore */ }
const DEFAULT_EXEC_TIMEOUT = 60 * 60 * 1000; // 1 hour


// Multer configuration for multipart file uploads
const uploadTempDir = path.join(os.tmpdir(), 'unity-uploads');
try {
  fs.mkdirSync(uploadTempDir, { recursive: true });
} catch (_e) {
  // ignore
}

const uploadMiddleware = multer({
  dest: uploadTempDir,
  limits: {
    fileSize: 500 * 1024 * 1024, // 500MB per file
    files: 100,
  },
});

function sanitizePath(filename: string, baseDir: string): string {
  const resolved = path.resolve(baseDir, filename);
  const normalizedBase = path.resolve(baseDir);
  if (!resolved.startsWith(normalizedBase + path.sep) && resolved !== normalizedBase) {
    throw new Error(`Path traversal blocked: ${filename}`);
  }
  return resolved;
}

async function ensureDir(dirPath: string): Promise<void> {
  await fs.promises.mkdir(dirPath, { recursive: true });
}

async function writeFileWithEncoding(
  filepath: string,
  content: string,
  encoding: 'text' | 'base64' = 'text'
): Promise<void> {
  await ensureDir(path.dirname(filepath));
  if (encoding === 'base64') {
    const buffer = Buffer.from(content, 'base64');
    await fs.promises.writeFile(filepath, buffer);
  } else {
    await fs.promises.writeFile(filepath, content, 'utf-8');
  }
}

async function readFileWithEncoding(
  filepath: string,
  encoding: 'text' | 'base64' = 'text'
): Promise<string> {
  const buffer = await fs.promises.readFile(filepath);
  return encoding === 'base64' ? buffer.toString('base64') : buffer.toString('utf-8');
}

interface ExecResult {
  exitCode: number;
  stdout: string;
  stderr: string;
  duration: number;
}

type ShellMode = 'cmd' | 'powershell';

function getShellConfig(shellMode: ShellMode): string | boolean {
  const isWindows = process.platform === 'win32';

  if (!isWindows) {
    return true;  // Use default /bin/sh on Unix
  }

  if (shellMode === 'cmd') {
    return 'cmd.exe';
  }

  // PowerShell (default on Windows)
  return 'powershell.exe';
}

function executeCommand(
  command: string,
  cwd: string,
  timeout: number,
  shellMode: ShellMode = 'powershell',
  onSpawn?: (proc: ChildProcess) => void,
): Promise<ExecResult> {
  return new Promise((resolve) => {
    const startTime = Date.now();
    let stdout = '';
    let stderr = '';
    let killed = false;

    // Detached on POSIX so the shell leads its own process group: steering
    // signals (see execControl) and the post-exit sweep reach the whole
    // pipeline, not just the shell.
    const detached = process.platform !== 'win32';
    const proc = spawn(command, [], {
      shell: getShellConfig(shellMode),
      cwd,
      timeout,
      detached,
    });
    onSpawn?.(proc);

    proc.stdout.on('data', (data) => {
      stdout += data.toString();
    });

    proc.stderr.on('data', (data) => {
      stderr += data.toString();
    });

    proc.on('error', (err) => {
      stderr += err.message;
    });

    proc.on('close', (code, signal) => {
      const duration = Date.now() - startTime;
      if (signal === 'SIGTERM') {
        killed = true;
        stderr += `\nProcess killed after ${timeout}ms timeout`;
      }
      if (detached && proc.pid != null) {
        // The shell is gone; sweep any group members it left behind.
        try {
          process.kill(-proc.pid, 'SIGKILL');
        } catch {
          // No survivors — the common case.
        }
      }
      resolve({
        exitCode: code ?? (killed ? 124 : 1),
        stdout,
        stderr,
        duration,
      });
    });
  });
}

function getDefaultBrowserPaths() {
  const downloadsPath = path.join(LOCAL_ROOT, 'Downloads');
  const tracesDir = path.join(LOCAL_ROOT, 'Traces');
  return { downloadsPath, tracesDir };
}

const defaultBrowserPaths = getDefaultBrowserPaths();

const app = express();
const wsInstance = expressWs(app);
app.use(express.json({ limit: '100mb' }));

const ALLOWED_ORIGINS = (process.env.CORS_ALLOWED_ORIGINS || '').split(',').filter(Boolean);
app.use((req, res, next) => {
  const origin = req.headers.origin;
  if (origin && ALLOWED_ORIGINS.includes(origin)) {
    res.setHeader('Access-Control-Allow-Origin', origin);
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Authorization, Content-Type');
    res.setHeader('Access-Control-Allow-Credentials', 'true');
  }
  if (req.method === 'OPTIONS') {
    return res.sendStatus(204);
  }
  next();
});

// --- Authorization (Bearer) middleware ---
function verifyApiKeyWithUnify(apiKey: string): Promise<boolean> {
  return new Promise((resolve) => {
    const url = new URL(`${process.env.ORCHESTRA_URL}/user/basic-info`);
    const options = {
      method: 'GET',
      hostname: url.hostname,
      port: url.port || (url.protocol === 'https:' ? 443 : 80),
      path: url.pathname + url.search,
      headers: {
        Authorization: `Bearer ${apiKey}`,
      },
    };

    const requestLib = url.protocol === 'https:' ? https : http;
    const req = requestLib.request(options, (res) => {
      const code = res.statusCode || 0;
      let body = '';
      res.on('data', (chunk) => { body += chunk; });
      res.on('end', () => {
        if (!(code >= 200 && code < 300)) return resolve(false);
        return resolve(true);
      });
    });
    req.on('error', () => {
      resolve(false);
    });
    req.end();
  });
}

async function auth(req: Request, res: Response, next: Function) {
  const authHeader = req.header('authorization') || '';
  const match = authHeader.match(/^Bearer\s+(.+)$/i);
  if (!match) {
    return res.status(401).json({ error: 'unauthorized', message: 'Missing or invalid API key' });
  }
  const apiKey = match[1].trim();

  // Check 1: Bearer token must match UNIFY_KEY
  if (apiKey !== process.env.UNIFY_KEY) {
    return res.status(401).json({ error: 'unauthorized', message: 'Invalid API key' });
  }

  // Check 2: Verify with /user/basic-info endpoint
  try {
    const ok = await verifyApiKeyWithUnify(apiKey);
    if (!ok) {
      return res.status(401).json({ error: 'unauthorized', message: 'API key verification failed' });
    }
  } catch (e) {
    return res.status(401).json({ error: 'unauthorized', message: 'API key verification failed' });
  }

  next();
}

app.use(auth);

// --- CLI argument parsing ---
function parseIntArg(flag: string, defaultValue: number): number {
  const idx = process.argv.indexOf(flag);
  if (idx !== -1 && idx + 1 < process.argv.length) {
    const val = parseInt(process.argv[idx + 1], 10);
    return isNaN(val) ? defaultValue : val;
  }
  return defaultValue;
}

const ACT_HISTORY_DEPTH = parseIntArg('--history-depth', 5);
console.log(`[memory-carryover] Act history depth: ${ACT_HISTORY_DEPTH}`);

// --- Session registry ---
interface ActHistoryEntry {
  task: string;
  observations: Observation[];
}

interface SessionInfo {
  agent: BrowserAgent;
  mode: 'web' | 'desktop' | 'web-vm';
  createdAt: Date;
  lastAccessed: Date;
  actHistory: ActHistoryEntry[];
  latestScreenshot: string;
  latestCursorPosition: { x: number; y: number } | null;
  /** Present for web-vm and desktop sessions; drives native X11 input/output. */
  displayHarness?: DisplayHarness;
  /** Maps LLM observation coordinates to full X11 display coordinates. */
  nativeObservationScale?: NativeObservationScale;
  /** Model/policy used to derive observation dimensions for this session. */
  observationModel: string;
  observationPolicy: ObservationScalingPolicy;
}

function cacheScreenshot(sessionId: string, screenshot: string, cursorPosition: { x: number; y: number } | null) {
  const session = activeSessions.get(sessionId);
  if (session && screenshot) {
    session.latestScreenshot = screenshot;
    session.latestCursorPosition = cursorPosition;
  }
}

function refreshDesktopCache(triggerSessionId: string) {
  const triggerSession = activeSessions.get(triggerSessionId);
  if (!triggerSession || triggerSession.mode !== "web-vm") return;
  const desktopEntry = [...activeSessions.entries()].find(([, s]) => s.mode === "desktop");
  if (!desktopEntry) return;
  const [deskId, deskSession] = desktopEntry;
  (async () => {
    try {
      if (deskSession.displayHarness) {
        const { observationB64 } = await captureNativeObservation(deskSession);
        cacheScreenshot(deskId, observationB64, null);
      } else {
        const connector = deskSession.agent.require(BrowserConnector);
        const harness = connector.getHarness();
        const rawImage = await harness.screenshot();
        const image = await connector.transformScreenshot(rawImage);
        const deskScreenshot = await image.toBase64();
        cacheScreenshot(deskId, deskScreenshot, harness.getCursorPosition());
      }
    } catch (err) {
      console.warn(`[cache] Desktop screenshot refresh failed: ${err}`);
    }
  })();
}

const activeSessions = new Map<string, SessionInfo>();
const SESSION_TIMEOUT_MS = 30 * 60 * 1000; // 30 minutes

// Cleanup inactive sessions periodically
setInterval(() => {
  const now = Date.now();
  for (const [sessionId, session] of activeSessions.entries()) {
    if (now - session.lastAccessed.getTime() > SESSION_TIMEOUT_MS) {
      console.log(`Cleaning up inactive session: ${sessionId}`);
      session.agent.stop().catch((err: unknown) => console.error(`Error stopping session ${sessionId}:`, err));
      activeSessions.delete(sessionId);
      broadcastSessionEvent(sessionId, 'timeout');
    }
  }
}, 5 * 60 * 1000); // Check every 5 minutes

const port = process.env.PORT || 3000;

// --- WebSocket Log Broadcasting Logic ---
const logClients = new Set<WebSocket>();

function broadcastLog(message: string) {
  logClients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(message);
    }
  });
}

function broadcastSessionEvent(sessionId: string, reason: string) {
  broadcastLog(JSON.stringify({ __type: 'session:closed', sessionId, reason }));
}

// Monkey-patch console methods to capture and broadcast logs
const originalLog = console.log;
const originalError = console.error;
const originalWarn = console.warn;

console.log = (...args: any[]) => {
  const message = util.format(...args);
  broadcastLog(message);
  originalLog.apply(console, args);
};

console.error = (...args: any[]) => {
  const message = util.format(...args);
  broadcastLog(message);
  originalError.apply(console, args);
};

console.warn = (...args: any[]) => {
  const message = util.format(...args);
  broadcastLog(message);
  originalWarn.apply(console, args);
};

// --- WebSocket Endpoint Handler ---
wsInstance.app.ws('/logs/stream', async (ws: WebSocket, req: Request) => {
  // Authenticate WebSocket connection
  const authHeader = req.header('authorization') || '';
  const match = authHeader.match(/^Bearer\s+(.+)$/i);

  if (!match) {
    console.log('WebSocket connection rejected: No auth header');
    ws.close(1008, 'Missing or invalid API key');
    return;
  }

  const apiKeyRaw = match[1].trim();
  const apiKey = apiKeyRaw.split(/\s+/)[0];

  // Check 1: Bearer token must match UNIFY_KEY
  if (apiKey !== process.env.UNIFY_KEY) {
    console.log('WebSocket connection rejected: Invalid API key');
    ws.close(1008, 'Invalid API key');
    return;
  }

  // Check 2: Verify with /user/basic-info endpoint
  try {
    const ok = await verifyApiKeyWithUnify(apiKey);
    if (!ok) {
      console.log('WebSocket connection rejected: Auth failed');
      ws.close(1008, 'API key verification failed');
      return;
    }
  } catch (e) {
    console.log('WebSocket connection rejected: Auth error');
    ws.close(1008, 'API key verification failed');
    return;
  }

  console.log('Log stream client connected and authenticated.');
  logClients.add(ws);

  ws.on('close', () => {
    console.log('Log stream client disconnected.');
    logClients.delete(ws);
  });

  ws.on('error', (error: Error) => {
    console.error('Log stream client error:', error);
    logClients.delete(ws);
  });
});


// --- Demo Sites ---
const DEMO_SITE_BASE_PORT = 4001;
const demoSiteProcesses: Map<number, ChildProcess> = new Map();

function isPortOpen(port: number): Promise<boolean> {
  return new Promise((resolve) => {
    const sock = net.createConnection({ port, host: '127.0.0.1' });
    sock.setTimeout(500);
    sock.on('connect', () => { sock.destroy(); resolve(true); });
    sock.on('error', () => { sock.destroy(); resolve(false); });
    sock.on('timeout', () => { sock.destroy(); resolve(false); });
  });
}

function waitForPort(port: number, timeoutMs = 5000): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  return new Promise((resolve) => {
    const check = async () => {
      if (await isPortOpen(port)) return resolve(true);
      if (Date.now() >= deadline) return resolve(false);
      setTimeout(check, 200);
    };
    check();
  });
}

function findDemoSitesRoot(): string | null {
  // demo-sites/ lives inside agent-service/ so it's always co-located
  const candidates = [
    path.resolve(__dirname, '..', 'demo-sites'),           // dev: agent-service/src/../demo-sites
    path.resolve(__dirname, '..', '..', 'demo-sites'),     // compiled: agent-service/dist/../../demo-sites
    '/app/agent-service/demo-sites',                        // Docker
  ];
  for (const dir of candidates) {
    if (fs.existsSync(dir)) return dir;
  }
  return null;
}

async function findFreePort(startFrom: number): Promise<number> {
  let port = startFrom;
  while (await isPortOpen(port) || demoSiteProcesses.has(port)) {
    port++;
  }
  return port;
}

async function ensureDemoSites(urlMappings: Record<string, string>): Promise<Record<string, string>> {
  const resolved: Record<string, string> = {};
  const demoSitesRoot = findDemoSitesRoot();
  if (!demoSitesRoot) {
    console.warn('[demo-sites] No demo-sites directory found, skipping');
    return resolved;
  }

  let nextPort = DEMO_SITE_BASE_PORT;

  for (const [originalUrl, dirName] of Object.entries(urlMappings)) {
    const siteDir = path.join(demoSitesRoot, dirName);
    if (!fs.existsSync(siteDir)) {
      console.warn(`[demo-sites] Directory '${dirName}' not found in ${demoSitesRoot}, skipping`);
      continue;
    }

    const serverJs = path.join(siteDir, 'server.js');
    const indexHtml = path.join(siteDir, 'index.html');

    if (!fs.existsSync(serverJs) && !fs.existsSync(indexHtml)) {
      console.warn(`[demo-sites] ${dirName} has no server.js or index.html, skipping`);
      continue;
    }

    const port = await findFreePort(nextPort);
    nextPort = port + 1;

    if (fs.existsSync(serverJs)) {
      console.log(`[demo-sites] Starting ${dirName} on port ${port} (node server.js)`);
      const proc = spawn('node', [serverJs, String(port)], {
        cwd: siteDir,
        stdio: ['ignore', 'pipe', 'pipe'],
      });
      proc.stdout?.on('data', (d: Buffer) => console.log(`[demo-sites:${dirName}] ${d.toString().trim()}`));
      proc.stderr?.on('data', (d: Buffer) => console.error(`[demo-sites:${dirName}] ${d.toString().trim()}`));
      proc.on('exit', (code) => console.log(`[demo-sites] ${dirName} exited with code ${code}`));
      demoSiteProcesses.set(port, proc);
    } else {
      console.log(`[demo-sites] Starting static server for ${dirName} on port ${port}`);
      const staticServer = http.createServer((req, res) => {
        const filePath = path.join(siteDir, req.url === '/' ? 'index.html' : req.url || 'index.html');
        fs.readFile(filePath, (err, data) => {
          if (err) { res.writeHead(404); res.end('Not found'); return; }
          const ext = path.extname(filePath).toLowerCase();
          const mimeTypes: Record<string, string> = {'.html':'text/html','.css':'text/css','.js':'text/javascript','.json':'application/json','.png':'image/png','.jpg':'image/jpeg','.svg':'image/svg+xml'};
          res.writeHead(200, { 'Content-Type': mimeTypes[ext] || 'application/octet-stream' });
          res.end(data);
        });
      });
      staticServer.listen(port, '0.0.0.0');
      const fakeProc = { exitCode: null, kill: () => { staticServer.close(); } } as unknown as ChildProcess;
      demoSiteProcesses.set(port, fakeProc);
    }

    const ready = await waitForPort(port);
    if (ready) {
      console.log(`[demo-sites] ${dirName} ready on port ${port}`);
    } else {
      console.error(`[demo-sites] ${dirName} failed to start on port ${port} within timeout`);
    }

    const localhostUrl = `http://localhost:${port}`;
    let mappingKey: string;
    try {
      mappingKey = new URL(originalUrl).href;
    } catch {
      console.warn(`[demo-sites] Skipping invalid URL mapping for ${dirName}`);
      continue;
    }
    resolved[mappingKey] = localhostUrl;

    // /etc/hosts + Caddy setup so the real domain resolves to the demo site
    try {
      const origUrl = new URL(originalUrl);
      const origHost = origUrl.hostname;

      const hostsFile = fs.readFileSync('/etc/hosts', 'utf-8');
      if (!hostsFile.includes(origHost)) {
        fs.appendFileSync('/etc/hosts', `\n127.0.0.1 ${origHost}\n`);
        console.log(`[demo-sites] Added /etc/hosts entry: 127.0.0.1 ${origHost}`);
      } else {
        console.log(`[demo-sites] /etc/hosts already has entry for ${origHost}`);
      }

      if (origUrl.protocol === 'https:') {
        const caddyFile = fs.existsSync('/etc/caddy/Caddyfile')
          ? fs.readFileSync('/etc/caddy/Caddyfile', 'utf-8') : '';
        if (!caddyFile.includes(origHost + ' {')) {
          const caddyBlock = `\n${origHost} {\n    tls internal\n    reverse_proxy localhost:${port}\n}\n`;
          fs.appendFileSync('/etc/caddy/Caddyfile', caddyBlock);
          console.log(`[demo-sites] Added Caddy block: ${origHost} -> localhost:${port}`);
        } else {
          console.log(`[demo-sites] Caddy already has block for ${origHost}`);
        }
      }
    } catch (e) {
      console.warn(`[demo-sites] Could not configure hosts/Caddy for ${originalUrl}: ${e}`);
    }
  }

  // Reload Caddy if any new blocks were added
  try {
    if (fs.existsSync('/etc/caddy/Caddyfile')) {
      execSync('caddy reload --config /etc/caddy/Caddyfile --adapter caddyfile 2>&1', { timeout: 10000 });
      console.log('[demo-sites] Caddy reloaded with new demo site routes');
    }
  } catch (e) {
    console.warn(`[demo-sites] Caddy reload failed: ${e}`);
  }

  return resolved;
}

// Cleanup demo site processes on exit
function cleanupDemoSites() {
  for (const [port, proc] of demoSiteProcesses) {
    try { proc.kill(); } catch {}
    console.log(`[demo-sites] Stopped process on port ${port}`);
  }
  demoSiteProcesses.clear();
}
process.on('SIGTERM', cleanupDemoSites);
process.on('SIGINT', cleanupDemoSites);
process.on('exit', cleanupDemoSites);


// --- Agent Initialization ---
console.log(`Starting Magnitude BrowserAgent...`);
const root = express();
root.use('/api', app);
root.use(app);
root.listen(port, () => {
  console.log(`🚀 BrowserAgent service listening on http://localhost:${port}`);
  void probeNativeDisplayAtBoot();
});

const isAgentReady = (req: Request, res: Response, next: Function) => {
  let sessionId = req.body.sessionId;
  if (!sessionId) {
    // Desktop mode is singleton (one physical display, one session).
    // Callers that omit sessionId are targeting the desktop.
    const desktopEntry = [...activeSessions.entries()]
      .find(([, s]) => s.mode === "desktop");
    if (desktopEntry) {
      sessionId = desktopEntry[0];
      req.body.sessionId = sessionId;
    } else {
      return res.status(400).json({ error: 'no_desktop_session', message: 'No active desktop session. Call /start with mode=desktop first.' });
    }
  }
  const session = activeSessions.get(sessionId);
  if (!session) {
    return res.status(404).json({ error: 'session_not_found', message: `Session ${sessionId} not found.` });
  }
  session.lastAccessed = new Date();
  next();
};

const getLaunchOptions = (
  headless: boolean,
  downloadsPath: string | null = null,
  tracesDir: string | null = null,
  storageStateName: string | null = null,
  stealth: boolean = false,
  egress: ResolvedEgress | null = null,
) => {
  // ``storageStateName`` is forwarded to magnitude-core's BrowserProvider,
  // which loads ~/.magnitude/browser_states/<safeName>.json (cookies +
  // localStorage + sessionStorage) before any page renders.  Used by
  // brain.influencers.youtube to keep one operator-supervised Google
  // login persistent across subsequent headless extraction runs.
  const opts: any = {
    launchOptions: {
      headless: headless,
      args: [
        "--disable-blink-features=AutomationControlled",
        "--disable-features=IsolateOrigins,site-per-process",
        // "--enable-features=WebRtcV4L2VideoCapture",
        // "--auto-select-window-capture-source-by-title=Google",
        '--auto-select-desktop-capture-source="Entire screen"',
      ],
      downloadsPath: downloadsPath || undefined,
      tracesDir: tracesDir || undefined,
    },
  };
  if (storageStateName) {
    opts.storageStateName = storageStateName;
  }
  // Opt-in anti-automation hardening (magnitude-core BrowserProvider applies
  // it; see web/stealth.ts). Off unless the caller asks or MAGNITUDE_STEALTH
  // is set in the process env.
  if (stealth) {
    opts.stealth = true;
  }
  applyEgress(opts, egress);
  return opts;
};

/**
 * Fold a resolved egress policy into magnitude's browser options.
 *
 * Proxy, WebRTC containment args and the region-derived context all come from
 * one resolution so they cannot drift apart: a proxied session still reporting
 * the host's timezone is a worse signal than an unproxied one.
 */
const applyEgress = (opts: any, egress: ResolvedEgress | null | undefined) => {
  if (!egress || !egress.proxy) return;
  opts.launchOptions = opts.launchOptions || {};
  opts.launchOptions.proxy = egress.proxy;
  opts.launchOptions.args = [...(opts.launchOptions.args || []), ...egress.args];
  if (Object.keys(egress.contextOptions).length > 0) {
    opts.contextOptions = { ...(opts.contextOptions || {}), ...egress.contextOptions };
  }
};

const startDesktop = async (): Promise<BrowserAgent> => {
  try {
    await waitForLocalNoVnc();
    const desktopUrl = buildDesktopNoVncUrl();
    const desktopOrigin = new URL(desktopUrl).origin;
    const agent = await startBrowserAgent({
      url: desktopUrl,
      browser: getLaunchOptions(true),
      prompt: "You're controlling a noVNC virtual desktop page. Do not navigate to other page and use mouse and keyboard to control the browser and apps within the virtual desktop. There may be a terminal (xterm) app launched in the desktop for use.",
      narrate: true,
      llm: getLlmConfig()
    });
    agent.context.setDefaultNavigationTimeout(90000);
    // Auto-grant clipboard permissions so the noVNC "Share clipboard?" popup is suppressed
    await agent.context.grantPermissions(
      ['clipboard-read', 'clipboard-write'],
      { origin: desktopOrigin },
    );
    console.log("✅ Desktop BrowserAgent started successfully.");
    return agent;
  } catch (err) {
    console.error("❌ Failed to start Desktop BrowserAgent:", err);
    throw err;
  }
}

const startBrowser = async (
  headless: boolean,
  urlMappings?: Record<string, string>,
  storageStateName?: string,
  sessionMeta?: { sessionId?: string; sessionLabel?: string },
  stealth: boolean = false,
  egress: ResolvedEgress | null = null,
): Promise<BrowserAgent> => {
  try {
    const agent = await startBrowserAgent({
      url: "https://www.google.com/",
      browser: getLaunchOptions(
        headless,
        defaultBrowserPaths.downloadsPath,
        defaultBrowserPaths.tracesDir,
        storageStateName ?? null,
        stealth,
        egress,
      ),
      narrate: true,
      urlMappings,
      sessionId: sessionMeta?.sessionId,
      sessionLabel: sessionMeta?.sessionLabel,
      llm: getLlmConfig()
    });
    agent.context.setDefaultNavigationTimeout(90000);
    console.log("✅ BrowserAgent started successfully.");
    return agent;
  } catch (err) {
    console.error("❌ Failed to start BrowserAgent:", err);
    throw err;
  }
}

const startBrowserOnVm = async (
  urlMappings?: Record<string, string>,
  sessionMeta?: { sessionId?: string; sessionLabel?: string },
  egress: ResolvedEgress | null = null,
): Promise<BrowserAgent> => {
  try {
    const vmBrowserOptions: any = {
        launchOptions: {
          headless: false,
          args: [
            "--disable-blink-features=AutomationControlled",
            "--disable-features=IsolateOrigins,site-per-process",
            '--auto-select-desktop-capture-source="Entire screen"',
          ],
          downloadsPath: defaultBrowserPaths.downloadsPath || undefined,
          tracesDir: defaultBrowserPaths.tracesDir || undefined,
        },
        contextOptions: { viewport: null, ignoreHTTPSErrors: true },
    };
    applyEgress(vmBrowserOptions, egress);
    const agent = await startBrowserAgent({
      url: "https://www.google.com/",
      browser: vmBrowserOptions,
      narrate: true,
      urlMappings,
      sessionId: sessionMeta?.sessionId,
      sessionLabel: sessionMeta?.sessionLabel,
      llm: getLlmConfig()
    });
    agent.context.setDefaultNavigationTimeout(90000);
    console.log("✅ Web-VM BrowserAgent started successfully.");
    return agent;
  } catch (err) {
    console.error("❌ Failed to start Web-VM BrowserAgent:", err);
    throw err;
  }
}

// --- Google Meet browser launcher ---

app.post('/start', async (req: Request, res: Response) => {
  // ``storageStateName`` is optional. When set, the magnitude
  // BrowserProvider loads ~/.magnitude/browser_states/<safeName>.json
  // (cookies + localStorage + sessionStorage) before any page renders so
  // the new session boots already-authenticated. Currently only honoured
  // for ``mode === 'web'``.
  const { headless, mode, label, urlMappings, storageStateName, stealth, egress } = req.body;
  if (!mode || !['desktop', 'web', 'web-vm'].includes(mode)) {
    return res.status(400).json({
      error: 'bad_request',
      message:
        'Mode is required and must be "desktop", "web", or "web-vm".',
    });
  }

  // Desktop mode is singleton -- one physical display, one session.
  // Fully stop any existing desktop session before creating a new one so
  // Playwright does not tear down the browser context mid-start.
  if (mode === "desktop") {
    const stopTasks: Array<Promise<void>> = [];
    for (const [existingId, existing] of activeSessions.entries()) {
      if (existing.mode === "desktop") {
        console.log(`Replacing existing desktop session: ${existingId}`);
        stopTasks.push(
          existing.agent.stop().catch((err: unknown) => {
            console.error(`Error stopping old desktop session: ${err}`);
          }),
        );
        activeSessions.delete(existingId);
        broadcastSessionEvent(existingId, 'replaced');
      }
    }
    if (stopTasks.length > 0) {
      await Promise.all(stopTasks);
    }
  }

  const sessionId = randomUUID();

  // Resolve the egress policy before anything is launched. A policy that
  // cannot be honoured must fail the request rather than silently egress from
  // the host: a caller that asked for a specific exit and got the host's own
  // address is worse off than one that got an error, because it cannot tell.
  let resolvedEgress: ResolvedEgress | null = null;
  try {
    const policy = parseEgressPolicy(egress);
    resolvedEgress = policy ? resolveEgress({ sessionKey: sessionId, ...policy }) : null;
  } catch (err) {
    if (err instanceof EgressPolicyError) {
      console.error(`[start] egress policy rejected: ${err.message}`);
      return res.status(400).json({ error: 'invalid_egress_policy', message: err.message });
    }
    throw err;
  }
  if (resolvedEgress) {
    console.log(`[start] egress=${resolvedEgress.description}`);
  }

  const t0 = Date.now();
  console.log(`[start] BEGIN mode=${mode} sessionId=${sessionId}`);
  try {
    let agent: BrowserAgent;
    const rawMappings = urlMappings && typeof urlMappings === 'object' ? urlMappings as Record<string, string> : undefined;
    const resolvedMappings = rawMappings ? await ensureDemoSites(rawMappings) : undefined;
    const mappings = resolvedMappings && Object.keys(resolvedMappings).length > 0 ? resolvedMappings : undefined;

    if (mode === "desktop") {
      agent = await startDesktop();
    } else if (mode === "web-vm") {
      agent = await startBrowserOnVm(mappings, { sessionId, sessionLabel: label }, resolvedEgress);
    } else {
      agent = await startBrowser(
        headless ?? false,
        mappings,
        typeof storageStateName === 'string' && storageStateName ? storageStateName : undefined,
        { sessionId, sessionLabel: label },
        stealth === true,
        resolvedEgress,
      );
    }
    console.log(`[start] agent_created=${Date.now() - t0}ms mode=${mode}`);

    // ── Diagnostic logging for URL mapping debugging ────────────────────
    if (mappings) {
      console.log(`[url-map-diag] urlMappings received by agent: ${JSON.stringify(mappings)}`);

      // Verify each demo site is actually reachable right now
      for (const [original, replacement] of Object.entries(mappings)) {
        console.log(`[url-map-diag] Mapping: ${original} -> ${replacement}`);
        try {
          const testResp = await fetch(replacement, { redirect: 'manual' });
          console.log(`[url-map-diag] Fetch test ${replacement} -> status=${testResp.status}, headers=${JSON.stringify(Object.fromEntries([...testResp.headers.entries()].filter(([k]) => ['content-type','location','content-length'].includes(k.toLowerCase()))))}`);
        } catch (e) {
          console.error(`[url-map-diag] Fetch test ${replacement} -> FAILED: ${e}`);
        }
      }

      // Log all registered routes on the context (Playwright exposes them via internal state)
      try {
        // Check if magnitude registered any routes by inspecting the context
        const page = agent.page;
        console.log(`[url-map-diag] Current page URL after agent start: ${page.url()}`);
      } catch (e) {
        console.warn(`[url-map-diag] Could not read page URL: ${e}`);
      }

      // Add a catch-all diagnostic route that logs EVERY request the browser makes.
      // Uses route.fallback() so it doesn't interfere with magnitude's routes --
      // if magnitude's route already handled it, this won't fire.
      // If this DOES fire for a mapped URL, it means magnitude's route did NOT catch it.
      try {
        await agent.context.route('**/*', async (route: any) => {
          const req = route.request();
          const url = req.url();
          const isNav = req.isNavigationRequest();
          const method = req.method();
          const resourceType = req.resourceType();

          // Log all navigation requests + anything hitting a mapped domain
          const mappedEntries = Object.entries(mappings!);
          let matchInfo = 'no-match';
          for (const [orig] of mappedEntries) {
            const origHost = new URL(orig).hostname;
            if (url.includes(origHost)) {
              matchInfo = `matches-domain:${origHost}`;
              // This request matched a mapped domain but reached our fallback,
              // meaning magnitude's context.route() did NOT intercept it.
              console.warn(`[url-map-diag] ⚠️ LEAKED REQUEST: ${method} ${url} (magnitude route did NOT intercept this)`);
              // Check if URL exactly matches what magnitude should catch
              const urlObj = new URL(url);
              console.warn(`[url-map-diag]   url.href=${urlObj.href}, original=${orig}, startsWith(orig+/)=${urlObj.href.startsWith(orig + '/')}, equals=${urlObj.href === orig}`);
              break;
            }
          }

          if (isNav) {
            console.log(`[url-map-diag] NAV ${method} ${url} (type=${resourceType}, ${matchInfo})`);
          }

          await route.fallback();
        });
        console.log(`[url-map-diag] Diagnostic catch-all route installed`);
      } catch (e) {
        console.warn(`[url-map-diag] Failed to install diagnostic route: ${e}`);
      }
    } else {
      console.log(`[url-map-diag] No urlMappings provided for this session`);
    }
    // ── End diagnostic logging ───────────────────────────────────────────

    if (label && mode === 'web-vm') {
      try {
        await agent.context.addInitScript(`
          (function() {
            function _injectBadge() {
              if (document.getElementById('__mag_session_badge')) return;
              var b = document.createElement('div');
              b.id = '__mag_session_badge';
              b.textContent = ${JSON.stringify(String(label))};
              b.style.cssText = 'position:fixed;top:4px;right:4px;z-index:2147483647;'
                + 'background:rgba(30,30,30,0.85);color:#fff;padding:2px 8px;'
                + 'font:bold 12px/16px system-ui,sans-serif;border-radius:4px;'
                + 'pointer-events:none;user-select:none;';
              (document.body || document.documentElement).appendChild(b);
            }
            if (document.body) _injectBadge();
            else document.addEventListener('DOMContentLoaded', _injectBadge);
          })();
        `);
      } catch (badgeErr) {
        console.warn(`[start] Badge injection failed: ${badgeErr}`);
      }
    }

    let displayHarness: DisplayHarness | undefined;
    if (mode === 'web-vm' || mode === 'desktop') {
      displayHarness = new DisplayHarness();
    }

    const observationModel = resolveAgentServiceModel();
    const observationPolicy = resolveObservationScalingPolicy(observationModel);

    activeSessions.set(sessionId, {
      agent,
      mode,
      createdAt: new Date(),
      lastAccessed: new Date(),
      actHistory: [],
      latestScreenshot: '',
      latestCursorPosition: null,
      displayHarness,
      observationModel,
      observationPolicy,
    });

    console.log(`[start] DONE mode=${mode} sessionId=${sessionId} total=${Date.now() - t0}ms active_sessions=${activeSessions.size}`);
    res.json({ status: 'started', sessionId });
  } catch (err) {
    console.error(`[start] ERROR mode=${mode} after ${Date.now() - t0}ms:`, err);
    handleAgentError(err, res);
  }
});

app.post('/nav', isAgentReady, async (req: Request, res: Response) => {
  const { url, sessionId } = req.body;
  if (!url) return res.status(400).json({ error: 'bad_request', message: 'URL is required.' });
  try {
    const session = activeSessions.get(sessionId)!;
    await session.agent.nav(url);
    res.json({ status: 'navigated', url });
  } catch (err) {
    handleAgentError(err, res);
  }
});

app.post('/act', isAgentReady, async (req: Request, res: Response) => {
  const { task, sessionId, lineage, verify } = req.body;
  if (!task) return res.status(400).json({ error: 'bad_request', message: 'Task description is required.' });
  try {
    const session = activeSessions.get(sessionId)!;
    const agent = session.agent;
    const actId = makeActId(task);

    const lineageLabel = Array.isArray(lineage) && lineage.length > 0
      ? `[${lineage.join('->')}->desktop.act] `
      : '[desktop.act] ';

    const memory = new AgentMemory({ promptCaching: true });

    // Fresh web/web-vm sessions already have a browser open and loaded.
    // Tell the LLM so it can no-op (return an empty action list) if the
    // task is simply asking to open a browser.
    if (session.actHistory.length === 0 && session.mode !== 'desktop') {
      memory.recordObservation(new Observation(
        'thought' as any,
        'user',
        'This is a freshly created browser session — the browser is already open and loaded. '
        + 'If the task is simply asking to open a browser, open a new browser window, or launch a browser, '
        + 'this has already been accomplished. Return an empty actions list.'
      ));
    }

    if (session.actHistory.length > 0) {
      let injectedCount = 0;
      for (const entry of session.actHistory) {
        memory.recordObservation(new Observation(
          'thought' as any,
          'user',
          `Previously completed task: "${entry.task}"`
        ));
        injectedCount++;
        for (const obs of entry.observations) {
          memory.recordObservation(obs);
          injectedCount++;
        }
      }
      console.log(`${lineageLabel}📋 Injecting history from ${session.actHistory.length} previous acts (${injectedCount} observations)`);
    } else {
      console.log(`${lineageLabel}📋 No prior act history in session`);
    }

    const boundary = memory.observationCount;

    const actT0 = Date.now();
    console.log(`${lineageLabel}🧠 Planning actions for: "${task}"${verify ? ' (verify=true)' : ''}`);

    const actActions = verify
      ? agent.actions
      : agent.actions.filter(a => !a.name.startsWith('task:'));
    const MAX_VERIFY_ITERATIONS = 5;
    const actionTraces: any[] = [];
    const iterationReasonings: string[] = [];
    const iterationPlannedActions: any[][] = [];
    let totalActionsExecuted = 0;

    for (let iteration = 0; iteration < (verify ? MAX_VERIFY_ITERATIONS : 1); iteration++) {
      if (iteration > 0) {
        console.log(`${lineageLabel}🔄 Verify pass ${iteration + 1}: re-observing and re-planning...`);
      }

      if (session.displayHarness) {
        // Downscale to LLM observation space; scale coords back up before xdotool.
        const { observation } = await captureNativeObservation(session);
        memory.recordObservation(
          Observation.fromConnector('web', observation, { type: 'screenshot', limit: 2, dedupe: true }),
        );
        console.log(`${lineageLabel}📸 native observation recorded`);
      } else {
        await agent.recordConnectorObservations(memory);
      }

      if (MAGNITUDE_DEBUG) {
        try {
          if (session.displayHarness) {
            const b64 = await session.displayHarness.screenshot();
            debugSaveImage(actId, iteration === 0 ? 'planning_screenshot' : `verify_${iteration}_screenshot`, b64);
          } else {
            const harness = agent.require(BrowserConnector).getHarness();
            const planImg = await harness.screenshot();
            debugSaveImage(actId, iteration === 0 ? 'planning_screenshot' : `verify_${iteration}_screenshot`, await planImg.toBase64());
          }
        } catch (debugErr) {
          console.warn(`[debug] Pre-plan screenshot capture failed: ${debugErr}`);
        }
      }

      const context = await agent.buildContext(memory);
      const { reasoning, actions } = await agent.models.partialAct(context, task, [], actActions);

      const planMs = Date.now() - actT0;
      console.log(`${lineageLabel}💭 Reasoning [${planMs}ms]: ${reasoning}`);
      console.log(`${lineageLabel}📋 Planned ${actions.length} action(s): ${actions.map(a => a.variant).join(', ')}`);

      iterationReasonings.push(reasoning);
      iterationPlannedActions.push(actions);
      memory.recordThought(reasoning);

      let hadNativeActions = false;
      for (let i = 0; i < actions.length; i++) {
        const action = actions[i];
        const actionDef = agent.identifyAction(action);
        const rendered = actionDef.render(action);
        const detail = JSON.stringify(action);
        console.log(`${lineageLabel}🛠️ Action ${totalActionsExecuted + i + 1}: ${rendered} ${detail}`);

        const actionT0 = Date.now();
        let actionError: string | undefined;
        try {
          if (session.displayHarness && NATIVE_ACTION_VARIANTS.has(action.variant)) {
            await dispatchNativeAction(
              session.displayHarness,
              action,
              session.nativeObservationScale,
            );
            memory.recordObservation(
              Observation.fromActionTaken(action.variant, JSON.stringify(action)),
            );
            hadNativeActions = true;
          } else {
            await agent.exec(action, memory);
          }
        } catch (err) {
          actionError = err instanceof Error ? err.message : String(err);
          throw err;
        } finally {
          const actionMs = Date.now() - actionT0;
          console.log(`${lineageLabel}✅ Completed ${action.variant} [${actionMs}ms]`);

          const actionTrace: any = {
            index: totalActionsExecuted + i,
            iteration,
            variant: action.variant,
            params: action,
            rendered,
            executionMs: actionMs,
          };
          if (actionError) actionTrace.error = actionError;

          if (MAGNITUDE_DEBUG) {
            try {
              let postB64: string;
              if (session.displayHarness) {
                postB64 = await session.displayHarness.screenshot();
              } else {
                const harness = agent.require(BrowserConnector).getHarness();
                const postImg = await harness.screenshot();
                postB64 = await postImg.toBase64();
              }
              const coordLabel = ('x' in action && 'y' in action)
                ? `_${action.x}_${action.y}`
                : ('from' in action && typeof action.from === 'object')
                  ? `_${action.from.x}_${action.from.y}`
                  : '';
              const padIdx = String(totalActionsExecuted + i + 1).padStart(3, '0');
              debugSaveImage(
                actId,
                `post_action/${padIdx}_${action.variant.replace(/:/g, '_')}${coordLabel}`,
                postB64,
              );
            } catch (debugErr) {
              console.warn(`[debug] Post-action screenshot failed: ${debugErr}`);
            }
          }

          actionTraces.push(actionTrace);
        }
      }

      // Inject one post-iteration screenshot into memory after all native
      // actions have been dispatched so the next planning step sees the
      // final display state without taking N screenshots per iteration.
      if (hadNativeActions && session.displayHarness) {
        const { observation } = await captureNativeObservation(session);
        memory.recordObservation(
          Observation.fromConnector('web', observation, { type: 'screenshot', limit: 2, dedupe: true }),
        );
      }

      totalActionsExecuted += actions.length;

      const taskDone = actions.some(a => a.variant === 'task:done');
      if (!verify || taskDone) break;
    }

    const totalMs = Date.now() - actT0;
    console.log(`${lineageLabel}🏁 ${totalActionsExecuted} action(s) executed across ${iterationReasonings.length} iteration(s) [${totalMs}ms]`);

    debugSaveTrace(actId, {
      actId,
      task,
      verify: !!verify,
      lineage: lineage ?? [],
      sessionMode: session.mode,
      sessionId,
      reasoning: iterationReasonings.join('\n---\n'),
      plannedActions: iterationPlannedActions,
      actionTraces,
      iterations: iterationReasonings.length,
      totalMs,
      historyDepth: session.actHistory.length,
      observationCountBefore: boundary,
    });

    const newObservations = memory.getObservationsSlice(boundary);
    const filtered = newObservations.filter(obs => {
      const src = obs.source;
      return src.startsWith('thought') || src.startsWith('action:taken:');
    });

    session.actHistory.push({ task, observations: filtered });
    if (session.actHistory.length > ACT_HISTORY_DEPTH) {
      session.actHistory = session.actHistory.slice(-ACT_HISTORY_DEPTH);
    }

    console.log(`[memory-carryover] Stored ${filtered.length} filtered observations for task "${task}" (history: ${session.actHistory.length}/${ACT_HISTORY_DEPTH})`);

    const thoughts = filtered
      .filter(obs => obs.source.startsWith('thought'))
      .map(obs => String(obs.content))
      .join('\n');

    let screenshot = '';
    try {
      if (session.displayHarness) {
        const { observationB64 } = await captureNativeObservation(session);
        screenshot = observationB64;
        cacheScreenshot(sessionId, screenshot, null);
      } else {
        const connector = session.agent.require(BrowserConnector);
        const harness = connector.getHarness();
        const rawImage = await harness.screenshot();
        const image = await connector.transformScreenshot(rawImage);
        screenshot = await image.toBase64();
        cacheScreenshot(sessionId, screenshot, harness.getCursorPosition());
      }
    } catch (screenshotErr) {
      console.warn(`[act] Post-act screenshot failed: ${screenshotErr}`);
    }

    res.json({ status: 'success', summary: thoughts, screenshot });
    refreshDesktopCache(sessionId);
  } catch (err) {
    handleAgentError(err, res);
  }
});

app.post('/execute-actions', isAgentReady, async (req: Request, res: Response) => {
  const { sessionId, actions } = req.body;
  if (!actions || !Array.isArray(actions) || actions.length === 0) {
    return res.status(400).json({
      error: 'bad_request',
      message: 'actions is required and must be a non-empty array of action objects.',
    });
  }

  try {
    const session = activeSessions.get(sessionId)!;
    const agent = session.agent;
    const t0 = Date.now();

    const variants = actions.map((a: any) => a.variant).join(', ');
    console.log(`[execute-actions] Executing ${actions.length} action(s) [${variants}] for session ${sessionId}`);

    if (session.displayHarness) {
      // Ensure observation scale is current before translating LLM coords.
      await captureNativeObservation(session);
      const scale = session.nativeObservationScale;
      const browserActions: any[] = [];
      for (const action of actions) {
        if (NATIVE_ACTION_VARIANTS.has(action.variant)) {
          if (browserActions.length > 0) {
            await agent.executeTrajectory(browserActions, { memory: agent.memory, recordObservations: false });
            browserActions.length = 0;
          }
          console.log(`[execute-actions] native: ${action.variant}`);
          await dispatchNativeAction(session.displayHarness, action, scale);
        } else {
          browserActions.push(action);
        }
      }
      if (browserActions.length > 0) {
        await agent.executeTrajectory(browserActions, { memory: agent.memory, recordObservations: false });
      }
    } else {
      await agent.executeTrajectory(actions, { memory: agent.memory, recordObservations: false });
    }

    const execMs = Date.now() - t0;
    console.log(`[execute-actions] ${actions.length} action(s) executed [${execMs}ms]`);

    let screenshot = '';
    let cursorPosition: { x: number; y: number } | null = null;
    try {
      if (session.displayHarness) {
        const { observationB64 } = await captureNativeObservation(session);
        screenshot = observationB64;
        cacheScreenshot(sessionId, screenshot, null);
      } else {
        const connector = agent.require(BrowserConnector);
        const harness = connector.getHarness();
        const rawImage = await harness.screenshot();
        const image = await connector.transformScreenshot(rawImage);
        screenshot = await image.toBase64();
        cursorPosition = harness.getCursorPosition();
        cacheScreenshot(sessionId, screenshot, cursorPosition);
      }
    } catch (screenshotErr) {
      console.warn(`[execute-actions] Post-execution screenshot failed: ${screenshotErr}`);
    }

    res.json({ status: 'success', screenshot, cursorPosition });
    refreshDesktopCache(sessionId);
  } catch (err) {
    handleAgentError(err, res);
  }
});

app.post('/extract', isAgentReady, async (req: Request, res: Response) => {
  const { instructions, schema, bypassDomProcessing, sessionId } = req.body;
  if (!instructions) {
    return res.status(400).json({ error: 'bad_request', message: 'Extraction instructions are required.' });
  }
  const maxRetries = 3;
  let lastError: unknown;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const zodSchema = schema ? jsonSchemaToZod(schema) : z.string();
      const session = activeSessions.get(sessionId)!;
      const shouldBypassDomProcessing =
        bypassDomProcessing === true || session.mode === 'desktop';

      // Desktop and web-vm sessions bypass DOM processing: DOM expansion is
      // meaningless for a full-display screenshot and destructive for noVNC.
      // Native sessions use a full-display screenshot for accurate extraction.
      if (shouldBypassDomProcessing || session.displayHarness) {
        let screenshot: Image;
        if (session.displayHarness) {
          const { observation } = await captureNativeObservation(session);
          screenshot = observation;
        } else {
          screenshot = await session.agent.require(BrowserConnector).getHarness().screenshot();
        }
        const data = await (session.agent.models as any).extract(instructions, zodSchema as ZodTypeAny, screenshot, '');
        return res.json({ data });
      } else {
        // Use the standard extraction method with DOM processing
        const data = await (session.agent as any).extract(instructions, zodSchema as ZodTypeAny);
        return res.json({ data });
      }
    } catch (err: unknown) {
      lastError = err;
      // Check if the error is related to the LLM returning invalid JSON.
      // Added a check for "Unexpected token" which can also indicate a JSON parsing issue.
      if (err instanceof Error && (err.message.includes('HTTP body is not JSON') || err.message.includes('Unexpected token'))) {
        console.warn(`Attempt ${attempt} failed with a transient JSON parsing error. Retrying in ${attempt}s...`);
        await sleep(attempt * 1000); // Wait a bit longer each time
      } else {
        // If it's a different error, fail immediately
        return handleAgentError(err, res);
      }
    }
  }

  // If all retries failed, handle the last recorded error
  console.error(`All ${maxRetries} retries failed for the extract request.`);
  handleAgentError(lastError, res);
});

app.post('/query', isAgentReady, async (req: Request, res: Response) => {
  const { query, schema, sessionId } = req.body;
  if (!query) {
    return res.status(400).json({ error: 'bad_request', message: 'Query is required.' });
  }
  try {
    const zodSchema: ZodTypeAny = schema ? jsonSchemaToZod(schema) : z.any();
    const session = activeSessions.get(sessionId)!;
    const data: unknown = await (session.agent as any).query(query, zodSchema);
    res.json({ data });
  } catch (err) {
    handleAgentError(err, res);
  }
});

// --- DisplayHarness: native X11 input/output via xdotool + scrot ---

async function downscaleDisplayScreenshotForObservation(
  displayB64: string,
  session: SessionInfo,
): Promise<{ observation: Image; observationB64: string }> {
  const displayImage = Image.fromBase64(displayB64);
  const { width, height } = await displayImage.getDimensions();
  const scale = computeNativeObservationScale(width, height, session.observationPolicy);
  session.nativeObservationScale = scale;

  if (
    scale.observationWidth === scale.displayWidth
    && scale.observationHeight === scale.displayHeight
  ) {
    return { observation: displayImage, observationB64: displayB64 };
  }

  const observation = await displayImage.resize(scale.observationWidth, scale.observationHeight);
  const observationB64 = await observation.toBase64();
  console.log(
    `[native-scale] model=${scale.model} provider=${scale.provider} `
    + `display=${scale.displayWidth}x${scale.displayHeight} `
    + `observation=${scale.observationWidth}x${scale.observationHeight}`,
  );
  return { observation, observationB64 };
}

async function captureNativeObservation(session: SessionInfo): Promise<{
  observation: Image;
  observationB64: string;
}> {
  const displayB64 = await session.displayHarness!.screenshot();
  const { observation, observationB64 } = await downscaleDisplayScreenshotForObservation(
    displayB64,
    session,
  );
  return { observation, observationB64 };
}

/**
 * Maps Playwright-style key names to xdotool key names where they differ.
 * Keys not listed here are passed through unchanged.
 */
const XDOTOOL_KEY_MAP: Record<string, string> = {
  ArrowDown: 'Down',
  ArrowUp: 'Up',
  ArrowLeft: 'Left',
  ArrowRight: 'Right',
  Enter: 'Return',
  Escape: 'Escape',
  Backspace: 'BackSpace',
  Delete: 'Delete',
  Tab: 'Tab',
  ' ': 'space',
  PageDown: 'Next',
  PageUp: 'Prior',
  Home: 'Home',
  End: 'End',
};

/**
 * Maps Playwright modifier key names to xdotool modifier names.
 */
const XDOTOOL_MODIFIER_MAP: Record<string, string> = {
  Control: 'ctrl',
  Meta: 'super',
  cmd: 'super',
  Alt: 'alt',
  Shift: 'shift',
};

/**
 * Resolve the X11 display for native input/screenshots.
 *
 * Pool VMs set ``DISPLAY=:1`` (TigerVNC on 5901); the local desktop Docker
 * image sets ``DISPLAY=:99``. Prefer the process env so both stacks work
 * without hardcoding a display number.
 */
function resolveNativeDisplay(): string {
  const raw = (process.env.DISPLAY || '').trim();
  if (!raw) {
    return ':99';
  }
  return raw.startsWith(':') ? raw : `:${raw}`;
}

/**
 * Log whether the configured X11 display and DisplayHarness binaries are usable.
 * Failures here are warnings only — sessions may still start before the desktop
 * is fully up — but a wrong DISPLAY shows up immediately in boot logs.
 */
async function probeNativeDisplayAtBoot(): Promise<void> {
  const display = resolveNativeDisplay();
  console.log(`[display] Using DISPLAY=${display} (from ${process.env.DISPLAY ? 'env' : 'default'})`);

  const required = ['xdotool', 'xdpyinfo'] as const;
  const screenshotTools = ['xfce4-screenshooter', 'scrot'] as const;
  const optional = ['wmctrl'] as const;

  for (const bin of required) {
    try {
      execSync(`command -v ${bin}`, { stdio: 'ignore' });
    } catch {
      console.warn(`[display] Missing required binary: ${bin}`);
    }
  }
  if (!screenshotTools.some((bin) => {
    try {
      execSync(`command -v ${bin}`, { stdio: 'ignore' });
      return true;
    } catch {
      return false;
    }
  })) {
    console.warn(
      `[display] Missing screenshot binary (need one of: ${screenshotTools.join(', ')})`,
    );
  }
  for (const bin of optional) {
    try {
      execSync(`command -v ${bin}`, { stdio: 'ignore' });
    } catch {
      console.warn(`[display] Missing optional binary: ${bin}`);
    }
  }

  try {
    const result = await executeCommand(
      `DISPLAY=${display} xdpyinfo >/dev/null`,
      LOCAL_ROOT,
      5_000,
    );
    if (result.exitCode === 0) {
      console.log(`[display] xdpyinfo OK on ${display}`);
    } else {
      console.warn(
        `[display] xdpyinfo failed on ${display} (exit ${result.exitCode}): ${result.stderr.trim()}`,
      );
    }
  } catch (err) {
    console.warn(`[display] xdpyinfo probe error on ${display}: ${err}`);
  }
}

/**
 * Routes mouse and keyboard actions through the host X11 display using
 * xdotool (input) and scrot/xfce4-screenshooter (screenshot capture).
 *
 * Used for web-vm and desktop sessions where Playwright controls a Chromium
 * window that is itself running on a virtual X11 display. Native input
 * bypasses Playwright's CDP layer so that actions land on browser chrome,
 * desktop windows, and other UI at absolute display coordinates. Mouse clicks
 * go to the topmost window at each coordinate; keyboard events go to the
 * current X11 focus target without raising Chromium first.
 *
 * The display number comes from ``DISPLAY`` (pool VMs use ``:1``; the local
 * desktop Docker image uses ``:99``).
 */
class DisplayHarness {
  private readonly display: string;

  constructor(display: string = resolveNativeDisplay()) {
    this.display = display;
  }

  private async execDisplay(command: string, timeoutMs = 10_000): Promise<ExecResult> {
    return executeCommand(
      `DISPLAY=${this.display} ${command}`,
      LOCAL_ROOT,
      timeoutMs,
    );
  }

  /** Capture the full virtual display as a base64-encoded PNG. */
  async screenshot(): Promise<string> {
    const dest = path.join(os.tmpdir(), `unity-display-${randomUUID()}.png`);
    try {
      const result = await this.execDisplay(
        `xfce4-screenshooter -f -s "${dest}" 2>/dev/null || scrot "${dest}"`,
        15_000,
      );
      if (result.exitCode !== 0 && !fs.existsSync(dest)) {
        throw new Error(`Native screenshot failed (exit ${result.exitCode}): ${result.stderr}`);
      }
      const buf = fs.readFileSync(dest);
      return buf.toString('base64');
    } finally {
      try { fs.unlinkSync(dest); } catch { /* best effort */ }
    }
  }

  async click(x: number, y: number, button: 'left' | 'right' | 'middle' = 'left'): Promise<void> {
    const btn = { left: 1, middle: 2, right: 3 }[button];
    await this.execDisplay(`xdotool mousemove --sync ${x} ${y} click ${btn}`);
  }

  async doubleClick(x: number, y: number): Promise<void> {
    await this.execDisplay(`xdotool mousemove --sync ${x} ${y} click --repeat 2 --delay 100 1`);
  }

  async drag(fromX: number, fromY: number, toX: number, toY: number): Promise<void> {
    await this.execDisplay(
      `xdotool mousemove --sync ${fromX} ${fromY} mousedown 1 ` +
      `sleep 0.1 mousemove --sync ${toX} ${toY} mouseup 1`,
    );
  }

  async scroll(x: number, y: number, deltaX: number, deltaY: number): Promise<void> {
    await this.execDisplay(`xdotool mousemove --sync ${x} ${y}`);
    // xdotool: button 4 = scroll up, 5 = scroll down, 6 = left, 7 = right
    const cmds: string[] = [];
    if (deltaY !== 0) {
      const btn = deltaY < 0 ? 4 : 5;
      const n = Math.max(1, Math.ceil(Math.abs(deltaY) / 100));
      cmds.push(`xdotool click --repeat ${n} --delay 50 ${btn}`);
    }
    if (deltaX !== 0) {
      const btn = deltaX < 0 ? 6 : 7;
      const n = Math.max(1, Math.ceil(Math.abs(deltaX) / 100));
      cmds.push(`xdotool click --repeat ${n} --delay 50 ${btn}`);
    }
    for (const cmd of cmds) {
      await this.execDisplay(cmd);
    }
  }

  async type(text: string): Promise<void> {
    const tmpFile = path.join(os.tmpdir(), `unity-type-${randomUUID()}.txt`);
    try {
      await fs.promises.writeFile(tmpFile, text, 'utf-8');
      await this.execDisplay(
        `xdotool type --clearmodifiers --delay 30 --file "${tmpFile}"`,
        30_000,
      );
    } finally {
      await fs.promises.unlink(tmpFile).catch(() => {});
    }
  }

  async key(key: string): Promise<void> {
    const xKey = XDOTOOL_KEY_MAP[key] ?? key;
    await this.execDisplay(`xdotool key --clearmodifiers ${xKey}`);
  }

  async hotkey(keys: string[]): Promise<void> {
    const xKeys = keys.map(k => XDOTOOL_MODIFIER_MAP[k] ?? XDOTOOL_KEY_MAP[k] ?? k);
    await this.execDisplay(`xdotool key --clearmodifiers ${xKeys.join('+')}`);
  }
}

// Dispatch a single action variant to a DisplayHarness.
async function dispatchNativeAction(
  harness: DisplayHarness,
  action: any,
  scale?: NativeObservationScale,
): Promise<void> {
  const mapPoint = (x: number, y: number) =>
    (scale ? scaleObservationCoordsToDisplay(x, y, scale) : { x, y });

  const v = action.variant as string;
  switch (v) {
    case 'mouse:click': {
      const { x, y } = mapPoint(action.x, action.y);
      await harness.click(x, y);
      break;
    }
    case 'mouse:double_click': {
      const { x, y } = mapPoint(action.x, action.y);
      await harness.doubleClick(x, y);
      break;
    }
    case 'mouse:right_click': {
      const { x, y } = mapPoint(action.x, action.y);
      await harness.click(x, y, 'right');
      break;
    }
    case 'mouse:drag': {
      const from = mapPoint(action.from.x, action.from.y);
      const to = mapPoint(action.to.x, action.to.y);
      await harness.drag(from.x, from.y, to.x, to.y);
      break;
    }
    case 'mouse:scroll': {
      const { x, y } = mapPoint(action.x, action.y);
      await harness.scroll(x, y, action.deltaX ?? 0, action.deltaY ?? 0);
      break;
    }
    case 'keyboard:type':
      await harness.type(action.content);
      break;
    case 'keyboard:enter':
      await harness.key('Return');
      break;
    case 'keyboard:tab':
      await harness.key('Tab');
      break;
    case 'keyboard:backspace':
      await harness.key('BackSpace');
      break;
    case 'keyboard:select_all':
      await harness.hotkey(['ctrl', 'a']);
      break;
    case 'keyboard:key': {
      const k = action.key as string;
      if (k.includes('+')) {
        await harness.hotkey(k.split('+').map((s: string) => s.trim()));
      } else {
        await harness.key(k);
      }
      break;
    }
    default:
      throw new Error(`dispatchNativeAction: unhandled variant "${v}"`);
  }
}

const NATIVE_ACTION_VARIANTS = new Set([
  'mouse:click',
  'mouse:double_click',
  'mouse:right_click',
  'mouse:drag',
  'mouse:scroll',
  'keyboard:type',
  'keyboard:enter',
  'keyboard:tab',
  'keyboard:backspace',
  'keyboard:select_all',
  'keyboard:key',
]);

let _screenshotInFlight = 0;

app.post('/screenshot', isAgentReady, async (req: Request, res: Response) => {
  const { sessionId } = req.body;
  _screenshotInFlight++;
  const t0 = Date.now();
  const session = activeSessions.get(sessionId)!;
  console.log(`[screenshot] START session=${sessionId} mode=${session.mode} in_flight=${_screenshotInFlight}`);
  try {
    if (session.displayHarness) {
      const tNative = Date.now();
      const { observationB64 } = await captureNativeObservation(session);
      const tCapture = Date.now();
      console.log(`[screenshot] native_capture=${tCapture - tNative}ms b64_len=${observationB64.length} total=${tCapture - t0}ms`);
      cacheScreenshot(sessionId, observationB64, null);
      res.json({ screenshot: observationB64, cursorPosition: null });
    } else {
      // Playwright path: page viewport screenshot with DPR normalisation and
      // optional aspect-ratio scaling for headless web sessions.
      const connector = session.agent.require(BrowserConnector);
      const harness = connector.getHarness();
      const tHarness = Date.now();
      console.log(`[screenshot] harness_acquired=${tHarness - t0}ms`);
      const rawImage = await harness.screenshot();
      const tCapture = Date.now();
      console.log(`[screenshot] playwright_capture=${tCapture - tHarness}ms`);
      const image = await connector.transformScreenshot(rawImage);
      const base64Image = await image.toBase64();
      const cursorPosition = harness.getCursorPosition();
      const tEncode = Date.now();
      console.log(`[screenshot] base64_encode=${tEncode - tCapture}ms b64_len=${base64Image.length} total=${tEncode - t0}ms`);
      cacheScreenshot(sessionId, base64Image, cursorPosition);
      res.json({ screenshot: base64Image, cursorPosition });
    }
    _screenshotInFlight--;
    console.log(`[screenshot] DONE total=${Date.now() - t0}ms in_flight=${_screenshotInFlight}`);
  } catch (err) {
    _screenshotInFlight--;
    console.error(`[screenshot] ERROR after ${Date.now() - t0}ms in_flight=${_screenshotInFlight}:`, err);
    handleAgentError(err, res, 'screenshot_failed');
  }
});

app.post('/screenshot/latest', isAgentReady, async (req: Request, res: Response) => {
  const { sessionId } = req.body;
  const session = activeSessions.get(sessionId)!;
  if (session.latestScreenshot) {
    res.json({ screenshot: session.latestScreenshot, cursorPosition: session.latestCursorPosition });
  } else {
    try {
      if (session.displayHarness) {
        const { observationB64 } = await captureNativeObservation(session);
        cacheScreenshot(sessionId!, observationB64, null);
        res.json({ screenshot: observationB64, cursorPosition: null });
      } else {
        const connector = session.agent.require(BrowserConnector);
        const harness = connector.getHarness();
        const rawImage = await harness.screenshot();
        const image = await connector.transformScreenshot(rawImage);
        const screenshot = await image.toBase64();
        const cursorPosition = harness.getCursorPosition();
        cacheScreenshot(sessionId!, screenshot, cursorPosition);
        res.json({ screenshot, cursorPosition });
      }
    } catch (err) {
      handleAgentError(err, res, 'screenshot_failed');
    }
  }
});

app.post('/eval', isAgentReady, async (req: Request, res: Response) => {
  const { sessionId, expression } = req.body;
  if (!expression) {
    return res.status(400).json({ error: 'bad_request', message: 'expression is required.' });
  }
  try {
    const session = activeSessions.get(sessionId)!;
    const harness = session.agent.require(BrowserConnector).getHarness();
    const result = await harness.page.evaluate(expression);
    res.json({ result });
  } catch (err) {
    handleAgentError(err, res, 'eval_failed');
  }
});

app.post('/viewport-info', isAgentReady, async (req: Request, res: Response) => {
  const { sessionId } = req.body;
  try {
    const session = activeSessions.get(sessionId)!;
    const harness = session.agent.require(BrowserConnector).getHarness();
    const page = harness.page;

    const playwrightViewport = page.viewportSize();

    const jsInfo = await page.evaluate(() => ({
      innerWidth: window.innerWidth,
      innerHeight: window.innerHeight,
      outerWidth: window.outerWidth,
      outerHeight: window.outerHeight,
      devicePixelRatio: window.devicePixelRatio,
      scrollX: window.scrollX,
      scrollY: window.scrollY,
      clientWidth: document.documentElement.clientWidth,
      clientHeight: document.documentElement.clientHeight,
      screenWidth: window.screen.width,
      screenHeight: window.screen.height,
      screenAvailWidth: window.screen.availWidth,
      screenAvailHeight: window.screen.availHeight,
    }));

    const screenshotBuffer = await page.screenshot({ type: 'png' });
    // PNG IHDR: width at bytes 16-19, height at bytes 20-23 (big-endian uint32)
    const rawScreenshotDims = {
      width: screenshotBuffer.readUInt32BE(16),
      height: screenshotBuffer.readUInt32BE(20),
    };

    console.log(`[viewport-info] mode=${session.mode} playwright=${JSON.stringify(playwrightViewport)} js=${JSON.stringify(jsInfo)} rawScreenshot=${JSON.stringify(rawScreenshotDims)}`);

    res.json({
      mode: session.mode,
      playwrightViewport,
      jsViewport: jsInfo,
      rawScreenshotDims,
      rescaledScreenshotDims: {
        width: Math.round(rawScreenshotDims.width / jsInfo.devicePixelRatio),
        height: Math.round(rawScreenshotDims.height / jsInfo.devicePixelRatio),
      },
    });
  } catch (err) {
    handleAgentError(err, res, 'viewport_info_failed');
  }
});

app.post('/state', isAgentReady, async (req: Request, res: Response) => {
  const { sessionId } = req.body;
  try {
    const session = activeSessions.get(sessionId)!;
    const page = session.agent.page;
    const url = page.url();
    const title = await page.title();
    res.json({ url, title });
  } catch (err) {
    handleAgentError(err, res, 'state_failed');
  }
});

// --- Helper: Get full page content with iframe expansion ---
async function getFullPageContentForExtraction(page: any): Promise<string> {
  // Get all iframe element handles
  const iframeHandles = await page.locator('iframe').elementHandles();

  // Iterate through each iframe handle and expand inline.
  //
  // Best-effort per iframe: the inline expansion runs an in-page
  // DOMParser/innerHTML write, which is (a) a live DOM *mutation* and (b) a
  // Trusted Types sink. Sites that enforce Trusted Types (e.g. LinkedIn) throw
  // at `parseFromString` — importantly BEFORE the `replaceChild`, so nothing is
  // mutated. We must not let one iframe abort the whole extraction: catch, skip
  // that iframe, and fall through to the passive `page.content()` below, which
  // serializes `documentElement.outerHTML` in the isolated world (no mutation,
  // no main-world script, not a Trusted Types sink).
  for (const iframeHandle of iframeHandles) {
    try {
      const frame = await iframeHandle.contentFrame();
      if (frame) {
        const iframeContent = await frame.content();
        await iframeHandle.evaluate((iframeNode: HTMLIFrameElement, { content }: { content: string }) => {
          const div = document.createElement('div');
          const parser = new DOMParser();
          const doc = parser.parseFromString(content, 'text/html');
          while (doc.body.firstChild) {
            div.appendChild(doc.body.firstChild);
          }
          const headElements = doc.head.querySelectorAll('style, link[rel="stylesheet"]');
          headElements.forEach(el => div.appendChild(el.cloneNode(true)));
          div.dataset.expandedFromIframe = 'true';
          div.dataset.iframeSrc = iframeNode.getAttribute('src') || '';
          iframeNode.parentNode?.replaceChild(div, iframeNode);
        }, { content: iframeContent });
      }
    } catch (err) {
      console.warn(`[content] iframe inline-expansion skipped (likely Trusted Types): ${err}`);
    }
  }

  return page.content();
}

// --- /links endpoint: Extract all links from current page ---
app.post('/links', isAgentReady, async (req: Request, res: Response) => {
  const { sessionId, sameDomain, selector } = req.body;
  try {
    const session = activeSessions.get(sessionId)!;
    const page = session.agent.page;
    const currentUrl = page.url();
    const currentHostname = new URL(currentUrl).hostname;

    // Extract all links via page.evaluate()
    const linkSelector = selector || 'a[href]';
    const links: Array<{ href: string; text: string }> = await page.evaluate((sel: string) => {
      return Array.from(document.querySelectorAll(sel))
        .map(a => ({
          href: (a as HTMLAnchorElement).href,
          text: (a as HTMLAnchorElement).innerText.trim().slice(0, 200)
        }))
        .filter(l => l.href && l.href.startsWith('http'));
    }, linkSelector);

    // Deduplicate by href
    const seen = new Set<string>();
    const uniqueLinks = links.filter(l => {
      if (seen.has(l.href)) return false;
      seen.add(l.href);
      return true;
    });

    // Optional: filter to same domain
    const filtered = sameDomain === true
      ? uniqueLinks.filter(l => {
          try {
            return new URL(l.href).hostname === currentHostname;
          } catch {
            return false;
          }
        })
      : uniqueLinks;

    res.json({
      base_url: new URL(currentUrl).origin,
      current_url: currentUrl,
      links: filtered,
      total: filtered.length
    });
  } catch (err) {
    handleAgentError(err, res, 'links_failed');
  }
});

// --- /content endpoint: Get raw page content (no LLM) ---
app.post('/content', isAgentReady, async (req: Request, res: Response) => {
  const { sessionId, format } = req.body;
  // format: 'html' | 'text' | 'markdown' (default: 'markdown')
  const outputFormat = format || 'markdown';

  try {
    const session = activeSessions.get(sessionId)!;
    const page = session.agent.page;
    const url = page.url();
    const title = await page.title();

    let content: string;

    if (outputFormat === 'text') {
      // Plain text extraction
      content = await page.innerText('body');
    } else if (outputFormat === 'html') {
      // Raw HTML with iframe expansion
      content = await getFullPageContentForExtraction(page);
    } else {
      // Markdown (default) - use magnitude-extract
      const htmlContent = await getFullPageContentForExtraction(page);

      const partitionOptions: PartitionOptions = {
        extractImages: true,
        extractForms: true,
        extractLinks: true,
        skipNavigation: false,
        minTextLength: 3,
        includeOriginalHtml: false,
        includeMetadata: true
      };

      const result = partitionHtml(htmlContent, partitionOptions);

      const markdownOptions: MarkdownSerializerOptions = {
        includeMetadata: false,
        includePageNumbers: false,
        includeElementIds: false,
        includeCoordinates: false,
        preserveHierarchy: true,
        escapeSpecialChars: true,
        includeFormFields: true,
        includeImageMetadata: true
      };

      content = serializeToMarkdown(result, markdownOptions);
    }

    res.json({ url, title, content, format: outputFormat });
  } catch (err) {
    handleAgentError(err, res, 'content_failed');
  }
});

// --- /captcha/solve endpoint: Delegate reCAPTCHA v2 to AntiCaptcha ---
//
// Extracts the sitekey from the live page, submits a RecaptchaV2TaskProxyless
// task to api.anti-captcha.com, polls for the worker-solved token, and
// injects it back into the page so the page's own submit flow accepts the
// verification. Returns once injection succeeds.
//
// The handler is deterministic and decoupled from magnitude-core's LLM
// action vocabulary: it is meant to be reached for by orchestration code
// after a separate ``observe()`` call has visually confirmed that a
// reCAPTCHA challenge is on screen.
//
// The token returned by AntiCaptcha is a Google-signed credential. It is
// NEVER logged, NEVER persisted, and NEVER echoed in the response body.
//
// The ``ANTICAPTCHA_KEY`` must be set in agent-service's own ``.env``; it
// is never accepted from the request body.
// --- Arkose Labs / FunCaptcha solve (best-effort) ---
//
// LinkedIn's login checkpoint uses Arkose FunCaptcha (the puzzle-piece "verify
// you're human"), NOT reCAPTCHA. AntiCaptcha supports it via a FunCaptchaTask,
// but sitekey extraction + token injection are inherently more heuristic than
// reCAPTCHA and Arkose Enterprise often binds the token to the solver's IP
// (proxyless can fail). This handler is therefore BEST-EFFORT: callers treat a
// non-'solved' result as "hand off to an operator". Extends the /captcha/solve
// route; reCAPTCHA v2 remains the default path.
async function solveArkoseFunCaptcha(
  sessionId: string,
  clientKey: string,
  res: Response,
  t0: number,
): Promise<Response> {
  let publicKey: string | null = null;
  let taskId: number | null = null;
  try {
    const session = activeSessions.get(sessionId)!;
    const page = session.agent.page;
    const pageUrl: string = page.url();

    // Extract the Arkose public key (pk) and, if present, the funcaptcha API
    // JS subdomain. Probe common shapes: a data-pkey attribute, the enforcement
    // config on window, script src, or the FunCaptcha iframe URL.
    const probe: { publicKey: string | null; subdomain: string | null } =
      await page.evaluate(() => {
        const pkFromEl = (document.querySelector('[data-pkey]') as HTMLElement | null)
          ?.getAttribute('data-pkey');
        if (pkFromEl) return { publicKey: pkFromEl, subdomain: null };

        const urls: string[] = [];
        document.querySelectorAll('script[src]').forEach((s) => {
          urls.push((s as HTMLScriptElement).getAttribute('src') || '');
        });
        document.querySelectorAll('iframe[src]').forEach((f) => {
          urls.push((f as HTMLIFrameElement).getAttribute('src') || '');
        });
        let subdomain: string | null = null;
        for (const raw of urls) {
          if (!raw) continue;
          try {
            const u = new URL(raw, window.location.href);
            if (/arkoselabs\.com|funcaptcha\.com|arkose/i.test(u.hostname)) {
              if (/api\.arkoselabs|-api\./i.test(u.hostname)) subdomain = u.hostname;
              const pk = u.searchParams.get('pk') || u.searchParams.get('public_key');
              if (pk) return { publicKey: pk, subdomain };
              const m = raw.match(/[0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12}/i);
              if (m) return { publicKey: m[0], subdomain };
            }
          } catch { /* skip */ }
        }
        const cfg: any = (window as any).ArkoseEnforcement || (window as any).arkose;
        const pk = cfg?.config?.publicKey || cfg?.publicKey || null;
        return { publicKey: pk, subdomain };
      });

    publicKey = probe.publicKey;
    if (!publicKey) {
      return res.status(400).json({
        error: 'no_sitekey',
        message: 'No Arkose/FunCaptcha public key was found on the current page.',
      });
    }

    const task: Record<string, unknown> = {
      type: 'FunCaptchaTaskProxyless',
      websiteURL: pageUrl,
      websitePublicKey: publicKey,
    };
    if (probe.subdomain) task.funcaptchaApiJSSubdomain = probe.subdomain;

    const createResp = await fetch('https://api.anti-captcha.com/createTask', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ clientKey, task }),
    });
    const createBody: any = await createResp.json().catch(() => ({}));
    if (!createResp.ok || createBody?.errorId !== 0) {
      console.error(
        `[captcha/solve] arkose createTask failed pk=${publicKey} ` +
        `httpStatus=${createResp.status} errorCode=${createBody?.errorCode}`,
      );
      return res.status(502).json({
        error: 'anticaptcha_api_error',
        message: `createTask failed: ${createBody?.errorCode || 'unknown'} - ${createBody?.errorDescription || ''}`,
      });
    }
    taskId = createBody.taskId;
    console.log(`[captcha/solve] arkose task_created task_id=${taskId} pk=${publicKey}`);

    let token: string | null = null;
    for (let attempt = 0; attempt < 80; attempt++) {
      await sleep(3000);
      const pollResp = await fetch('https://api.anti-captcha.com/getTaskResult', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ clientKey, taskId }),
      });
      const pollBody: any = await pollResp.json().catch(() => ({}));
      if (!pollResp.ok || pollBody?.errorId !== 0) {
        return res.status(502).json({
          error: 'anticaptcha_api_error',
          message: `getTaskResult failed: ${pollBody?.errorCode || 'unknown'}`,
        });
      }
      if (pollBody.status === 'ready') {
        token = pollBody.solution?.token || null;
        break;
      }
    }

    if (!token) {
      return res.status(504).json({
        error: 'solve_timeout',
        message: 'AntiCaptcha did not return a FunCaptcha token within ~4 minutes.',
      });
    }

    // Inject the Arkose token: fill the known token fields and invoke any
    // registered Arkose completion callback. Heuristic — Arkose integrations
    // vary — hence best-effort.
    const injected: boolean = await page.evaluate((tkn: string) => {
      let set = false;
      const selectors = [
        'input[name="fc-token"]',
        'input[name="verification-token"]',
        'input[name="arkose-token"]',
        '#FunCaptcha-Token',
        'input#fc-token',
      ];
      for (const sel of selectors) {
        document.querySelectorAll(sel).forEach((el) => {
          (el as HTMLInputElement).value = tkn;
          try { el.dispatchEvent(new Event('input', { bubbles: true })); } catch { /* best-effort */ }
          try { el.dispatchEvent(new Event('change', { bubbles: true })); } catch { /* best-effort */ }
          set = true;
        });
      }
      try {
        const ark: any = (window as any).arkose || (window as any).ArkoseEnforcement;
        const cb = ark?.config?.onCompleted || ark?.onCompleted;
        if (typeof cb === 'function') { cb({ token: tkn }); set = true; }
      } catch { /* best-effort */ }
      return set;
    }, token);

    let settledVia: 'networkidle' | 'timeout' = 'timeout';
    try {
      await page.waitForLoadState('networkidle', { timeout: 15_000 });
      settledVia = 'networkidle';
    } catch { settledVia = 'timeout'; }

    const solveTimeMs = Date.now() - t0;
    console.log(
      `[captcha/solve] arkose solved task_id=${taskId} pk=${publicKey} ` +
      `solve_time_ms=${solveTimeMs} injected=${injected} settled_via=${settledVia}`,
    );
    return res.json({
      status: injected ? 'solved' : 'token_uninjected',
      solve_time_ms: solveTimeMs,
      sitekey: publicKey,
      variant: 'arkose',
      task_id: taskId,
      injected,
      settled: settledVia !== 'timeout',
      settled_via: settledVia,
      note: 'Arkose/FunCaptcha solve is best-effort; verify the page advanced.',
    });
  } catch (err) {
    console.error(
      `[captcha/solve] arkose unexpected error task_id=${taskId} pk=${publicKey}: ` +
      `${err instanceof Error ? err.message : err}`,
    );
    handleAgentError(err, res, 'captcha_solve_failed');
    return res;
  }
}

app.post('/captcha/solve', isAgentReady, async (req: Request, res: Response) => {
  const { sessionId, variant: variantRaw } = req.body;
  const variant: 'v2_checkbox' | 'v2_invisible' | 'arkose' =
    variantRaw === 'arkose'
      ? 'arkose'
      : variantRaw === 'v2_invisible'
        ? 'v2_invisible'
        : 'v2_checkbox';

  const clientKey = process.env.ANTICAPTCHA_KEY;
  if (!clientKey) {
    return res.status(503).json({
      error: 'anticaptcha_key_missing',
      message: 'ANTICAPTCHA_KEY is not set in the agent-service environment.',
    });
  }

  const t0 = Date.now();

  // Arkose/FunCaptcha (e.g. LinkedIn login) uses a distinct task type +
  // injection path; reCAPTCHA v2 continues below.
  if (variant === 'arkose') {
    return await solveArkoseFunCaptcha(sessionId, clientKey, res, t0);
  }

  let sitekey: string | null = null;
  let taskId: number | null = null;

  try {
    const session = activeSessions.get(sessionId)!;
    const page = session.agent.page;
    const pageUrl: string = page.url();

    sitekey = await page.evaluate(() => {
      const decode = (raw: string | null): string | null => {
        if (!raw) return null;
        try { return decodeURIComponent(raw); } catch { return raw; }
      };
      const direct = document.querySelector('[data-sitekey]') as HTMLElement | null;
      const directKey = direct?.getAttribute('data-sitekey');
      if (directKey) return directKey;
      const iframes = Array.from(document.querySelectorAll('iframe')) as HTMLIFrameElement[];
      const probe = (substr: string): string | null => {
        for (const f of iframes) {
          const src = f.getAttribute('src') || '';
          if (src.includes(substr)) {
            try {
              const u = new URL(src, window.location.href);
              const k = u.searchParams.get('k');
              if (k) return decode(k);
            } catch { /* fall through */ }
          }
        }
        return null;
      };
      return probe('recaptcha/api2/anchor') || probe('recaptcha/api2/bframe');
    });

    if (!sitekey) {
      return res.status(400).json({
        error: 'no_sitekey',
        message: 'No reCAPTCHA sitekey was found on the current page.',
      });
    }

    const createResp = await fetch('https://api.anti-captcha.com/createTask', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        clientKey,
        task: {
          type: 'RecaptchaV2TaskProxyless',
          websiteURL: pageUrl,
          websiteKey: sitekey,
          isInvisible: variant === 'v2_invisible',
        },
      }),
    });
    const createBody: any = await createResp.json().catch(() => ({}));
    if (!createResp.ok || typeof createBody?.errorId !== 'number' || createBody.errorId !== 0) {
      console.error(
        `[captcha/solve] createTask failed sitekey=${sitekey} variant=${variant} ` +
        `httpStatus=${createResp.status} errorId=${createBody?.errorId} ` +
        `errorCode=${createBody?.errorCode}`,
      );
      return res.status(502).json({
        error: 'anticaptcha_api_error',
        message: `createTask failed: ${createBody?.errorCode || 'unknown'} - ${createBody?.errorDescription || ''}`,
        details: { errorId: createBody?.errorId, errorCode: createBody?.errorCode },
      });
    }
    taskId = createBody.taskId;
    console.log(`[captcha/solve] task_created task_id=${taskId} sitekey=${sitekey} variant=${variant}`);

    // Poll every 3s for up to 60 attempts (~3 min) for the worker pool to
    // return a token.  Anti-Captcha's docs recommend an initial 5s wait
    // before the first poll, but a 3s cadence from t=3s is fine and gives
    // us slightly faster turnaround on already-queued tasks.
    let token: string | null = null;
    for (let attempt = 0; attempt < 60; attempt++) {
      await sleep(3000);
      const pollResp = await fetch('https://api.anti-captcha.com/getTaskResult', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ clientKey, taskId }),
      });
      const pollBody: any = await pollResp.json().catch(() => ({}));
      if (!pollResp.ok || typeof pollBody?.errorId !== 'number' || pollBody.errorId !== 0) {
        console.error(
          `[captcha/solve] getTaskResult failed task_id=${taskId} ` +
          `httpStatus=${pollResp.status} errorId=${pollBody?.errorId} ` +
          `errorCode=${pollBody?.errorCode}`,
        );
        return res.status(502).json({
          error: 'anticaptcha_api_error',
          message: `getTaskResult failed: ${pollBody?.errorCode || 'unknown'} - ${pollBody?.errorDescription || ''}`,
          details: { errorId: pollBody?.errorId, errorCode: pollBody?.errorCode, taskId },
        });
      }
      if (pollBody.status === 'ready') {
        token = pollBody.solution?.gRecaptchaResponse || null;
        break;
      }
    }

    if (!token) {
      console.error(`[captcha/solve] solve_timeout task_id=${taskId} sitekey=${sitekey}`);
      return res.status(504).json({
        error: 'solve_timeout',
        message: 'AntiCaptcha worker pool did not return a token within ~3 minutes.',
      });
    }

    // Inject the token + invoke any registered callbacks, then poll the
    // reCAPTCHA widget's own JS API until it acknowledges the token. All
    // done inside a single ``page.evaluate`` so the token is passed in as
    // a function argument (and lives only on the page side) rather than
    // being serialised into the evaluation source string.
    //
    // The async polling loop turns the brittle "inject and pray" pattern
    // into a deterministic widget-level handshake: we know the widget
    // accepts the token when ``grecaptcha.getResponse()`` returns a
    // non-empty string. This catches injection failures (token written
    // but widget rejects it) AND eliminates the need for caller-side
    // sleeps.
    //
    // Returns ``{ injected, widgetAcked }`` where ``injected`` means at
    // least one textarea or callback received the token, and
    // ``widgetAcked`` means the widget's own JS API confirms it has
    // internalised the verification.
    const injectionResult: { injected: boolean; widgetAcked: boolean } = await page.evaluate(
      async (tkn: string) => {
        let textareaSet = false;
        let callbackCalled = false;

        const textareas = Array.from(
          document.querySelectorAll('textarea[id^="g-recaptcha-response"], textarea[name="g-recaptcha-response"]'),
        ) as HTMLTextAreaElement[];
        for (const ta of textareas) {
          ta.value = tkn;
          try { ta.dispatchEvent(new Event('input', { bubbles: true })); } catch { /* best-effort */ }
          try { ta.dispatchEvent(new Event('change', { bubbles: true })); } catch { /* best-effort */ }
          textareaSet = true;
        }

        // Strategy A: data-callback attribute names a window-scoped function.
        const cbHosts = Array.from(document.querySelectorAll('[data-callback]')) as HTMLElement[];
        for (const host of cbHosts) {
          const name = host.getAttribute('data-callback');
          if (!name) continue;
          const fn = (window as any)[name];
          if (typeof fn === 'function') {
            try { fn(tkn); callbackCalled = true; } catch { /* best-effort */ }
          }
        }

        // Strategy B: walk window.___grecaptcha_cfg.clients[*] for nested
        // ``callback`` functions (this is how SPA-mounted widgets register).
        try {
          const cfg: any = (window as any).___grecaptcha_cfg;
          const clients = cfg?.clients;
          if (clients && typeof clients === 'object') {
            const walk = (node: any, depth: number): void => {
              if (!node || depth > 6) return;
              if (typeof node === 'object') {
                for (const k of Object.keys(node)) {
                  const v = node[k];
                  if (k === 'callback' && typeof v === 'function') {
                    try { v(tkn); callbackCalled = true; } catch { /* best-effort */ }
                  } else if (typeof v === 'object' && v !== null) {
                    walk(v, depth + 1);
                  }
                }
              }
            };
            for (const clientKey of Object.keys(clients)) {
              walk(clients[clientKey], 0);
            }
          }
        } catch { /* best-effort */ }

        // Poll the widget's own ``grecaptcha.getResponse()`` until it
        // returns the injected token (or any non-empty string — some
        // Enterprise variants normalise the token). 5s ceiling.
        const widgetDeadline = Date.now() + 5_000;
        let widgetAcked = false;
        while (Date.now() < widgetDeadline) {
          try {
            const widget = (window as any).grecaptcha;
            const getResponse = widget && typeof widget.getResponse === 'function' ? widget.getResponse : null;
            if (getResponse) {
              const resp = getResponse();
              if (typeof resp === 'string' && resp.length > 0) {
                widgetAcked = true;
                break;
              }
            }
          } catch { /* best-effort */ }
          await new Promise(r => setTimeout(r, 100));
        }

        return { injected: textareaSet || callbackCalled, widgetAcked };
      },
      token,
    );

    if (!injectionResult.injected) {
      console.error(`[captcha/solve] injection_failed task_id=${taskId} sitekey=${sitekey}`);
      return res.status(500).json({
        error: 'injection_failed',
        message: 'Token retrieved but no textarea or callback was found on the page to receive it.',
      });
    }

    // Wait for the host page to actually progress past the captcha.
    // Two race-able signals, both Playwright-native, both bounded so no
    // misbehaved page can wedge the handler.  ``settled_via`` tells the
    // caller which signal latched first (or that we timed out).
    //
    // - 'userverify' — reCAPTCHA's server-side verification round-trip
    //   POSTs to ``recaptcha/api2/userverify`` (or the Enterprise
    //   variant).  Observing that response means Google has accepted
    //   the token; the host page can now act on it.
    // - 'networkidle' — Playwright reports the network as idle (no
    //   requests in flight for 500ms).  Catches the case where the
    //   verification call already completed before we started
    //   waiting, plus follow-up XHRs the host page fires after
    //   verification (e.g. "now fetch the revealed email").
    const SETTLE_TIMEOUT_MS = 15_000;
    let settledVia: 'userverify' | 'networkidle' | 'timeout' = 'timeout';
    try {
      settledVia = await Promise.race([
        page.waitForResponse(
          (r: { url: () => string }) => /recaptcha\/(api2|enterprise)\/userverify/.test(r.url()),
          { timeout: SETTLE_TIMEOUT_MS },
        ).then(() => 'userverify' as const),
        page.waitForLoadState('networkidle', { timeout: SETTLE_TIMEOUT_MS })
          .then(() => 'networkidle' as const),
      ]);
    } catch {
      // Both branches timed out — either the host page never went idle
      // (long-poll SPA) and never triggered userverify (challenge was
      // already pre-verified, or the page is wedged).  Return
      // settled=false so the caller can decide; we don't fail the
      // request because the token + injection are still valid.
      settledVia = 'timeout';
    }

    const solveTimeMs = Date.now() - t0;
    console.log(
      `[captcha/solve] solved task_id=${taskId} sitekey=${sitekey} variant=${variant} ` +
      `solve_time_ms=${solveTimeMs} widget_acked=${injectionResult.widgetAcked} ` +
      `settled_via=${settledVia}`,
    );
    res.json({
      status: 'solved',
      solve_time_ms: solveTimeMs,
      sitekey,
      variant,
      task_id: taskId,
      widget_acked: injectionResult.widgetAcked,
      settled: settledVia !== 'timeout',
      settled_via: settledVia,
    });
  } catch (err) {
    console.error(
      `[captcha/solve] unexpected error task_id=${taskId} sitekey=${sitekey}: ${err instanceof Error ? err.message : err}`,
    );
    handleAgentError(err, res, 'captcha_solve_failed');
  }
});

app.post('/stop', async (req: Request, res: Response) => {
  const { sessionId } = req.body;
  if (!sessionId) {
    return res.status(400).json({ error: 'bad_request', message: 'sessionId is required.' });
  }

  const session = activeSessions.get(sessionId);
  if (!session) {
    return res.status(404).json({ error: 'session_not_found', message: `Session ${sessionId} not found.` });
  }

  try {
    await session.agent.stop();
    activeSessions.delete(sessionId);
    broadcastSessionEvent(sessionId, 'stop');
    res.json({ status: 'stopped' });
    console.log(`BrowserAgent stopped for session ${sessionId}.`);
  } catch (err) {
    handleAgentError(err, res, 'stop_failed');
  }
});

app.post('/interrupt_action', isAgentReady, async (req: Request, res: Response) => {
  const { sessionId } = req.body;
  try {
    const session = activeSessions.get(sessionId)!;
    session.agent.interrupt();
    res.json({ status: 'interrupted', message: 'The current agent action has been interrupted.' });
  } catch (err) {
    handleAgentError(err, res, 'interrupt_failed');
  }
});

app.post('/pause', isAgentReady, async (req: Request, res: Response) => {
  const { sessionId } = req.body;
  try {
    const session = activeSessions.get(sessionId)!;
    session.agent.pause();
    res.json({ status: 'paused', message: 'The agent has been paused.' });
  } catch (err) {
    handleAgentError(err, res, 'pause_failed');
  }
});

app.post('/resume', isAgentReady, async (req: Request, res: Response) => {
  const { sessionId } = req.body;
  try {
    const session = activeSessions.get(sessionId)!;
    session.agent.resume();
    res.json({ status: 'resumed', message: 'The agent has been resumed.' });
  } catch (err) {
    handleAgentError(err, res, 'resume_failed');
  }
});

// --- /exec endpoint: Execute shell commands (use /files first to upload files) ---
const EXEC_ID_PATTERN = /^[A-Za-z0-9_-]{1,64}$/;

app.post('/exec', auth, async (req: Request, res: Response) => {
  const { command, cwd, timeout, shell_mode, exec_id } = req.body;
  // A caller that wants to steer the run supplies its own id, so it can
  // address /exec/signal at it while this request is still blocking.
  const execId = typeof exec_id === 'string' && EXEC_ID_PATTERN.test(exec_id)
    ? exec_id
    : randomUUID().slice(0, 8);

  if (!command || typeof command !== 'string') {
    return res.status(400).json({ error: 'bad_request', message: 'command is required and must be a string.' });
  }

  const workDir = cwd || LOCAL_ROOT;
  const execTimeout = typeof timeout === 'number' && timeout > 0 ? timeout : DEFAULT_EXEC_TIMEOUT;
  const shellMode: ShellMode = shell_mode === 'cmd' ? 'cmd' : 'powershell';

  try {
    const resolvedWorkDir = path.resolve(workDir);
    await ensureDir(resolvedWorkDir);

    console.log(`[exec] Running command: ${command} (cwd: ${resolvedWorkDir}, timeout: ${execTimeout}ms, shell: ${shellMode}, execId: ${execId})`);
    const result = await executeCommand(
      command,
      resolvedWorkDir,
      execTimeout,
      shellMode,
      (proc) => registerExec(execId, proc),
    );

    res.json({
      status: result.exitCode === 0 ? 'success' : 'error',
      exitCode: result.exitCode,
      stdout: result.stdout,
      stderr: result.stderr,
      duration: result.duration,
      cwd: resolvedWorkDir,
      execId,
    });
  } catch (err) {
    const errorMessage = err instanceof Error ? err.message : String(err);
    console.error(`[exec] Error: ${errorMessage}`);
    res.status(500).json({
      error: 'exec_failed',
      message: errorMessage,
      execId,
    });
  }
});

app.post('/exec/signal', auth, async (req: Request, res: Response) => {
  const { exec_id, action } = req.body;

  if (typeof exec_id !== 'string' || !EXEC_ID_PATTERN.test(exec_id)) {
    return res.status(400).json({ error: 'bad_request', message: 'exec_id is required and must be a string.' });
  }
  if (action !== 'stop' && action !== 'pause' && action !== 'resume') {
    return res.status(400).json({ error: 'bad_request', message: "action must be 'stop', 'pause', or 'resume'." });
  }

  const outcome = signalExec(exec_id, action);
  console.log(`[exec] signal ${action} for ${exec_id}: ${outcome}`);
  if (outcome === 'not_found') {
    return res.status(404).json({ error: 'not_found', message: 'No running exec with that id.' });
  }
  res.json({ status: outcome, exec_id, action });
});

// --- /files endpoint: Unified file management (JSON + Multipart) ---

// Handler for JSON requests
async function handleFilesJson(req: Request, res: Response) {
  const { action, files, filenames, path: subPath, filename, encoding } = req.body;

  if (!action || typeof action !== 'string') {
    return res.status(400).json({ error: 'bad_request', message: 'action is required.' });
  }

  const baseDir = LOCAL_ROOT;

  try {
    switch (action) {
      case 'save': {
        if (!Array.isArray(files) || files.length === 0) {
          return res.status(400).json({ error: 'bad_request', message: 'files array is required for save action.' });
        }

        const savedFiles: string[] = [];
        for (const file of files) {
          if (!file.filename || typeof file.filename !== 'string') {
            return res.status(400).json({ error: 'bad_request', message: 'Each file must have a filename.' });
          }
          if (typeof file.content !== 'string') {
            return res.status(400).json({ error: 'bad_request', message: 'Each file must have content.' });
          }

          const sanitizedPath = sanitizePath(file.filename, baseDir);
          const fileEncoding = file.encoding === 'base64' ? 'base64' : 'text';
          await writeFileWithEncoding(sanitizedPath, file.content, fileEncoding);
          savedFiles.push(file.filename);
          console.log(`[files] Saved: ${sanitizedPath}`);
        }

        return res.json({ status: 'saved', files: savedFiles });
      }

      case 'delete': {
        if (!Array.isArray(filenames) || filenames.length === 0) {
          return res.status(400).json({ error: 'bad_request', message: 'filenames array is required for delete action.' });
        }

        const deletedFiles: string[] = [];
        for (const fname of filenames) {
          if (typeof fname !== 'string') continue;
          const sanitizedPath = sanitizePath(fname, baseDir);
          try {
            await fs.promises.unlink(sanitizedPath);
            deletedFiles.push(fname);
            console.log(`[files] Deleted: ${sanitizedPath}`);
          } catch (err: any) {
            if (err.code !== 'ENOENT') throw err;
            // File doesn't exist, skip silently
          }
        }

        return res.json({ status: 'deleted', files: deletedFiles });
      }

      case 'list': {
        const listPath = subPath ? sanitizePath(subPath, baseDir) : baseDir;
        await ensureDir(listPath);

        const entries = await fs.promises.readdir(listPath, { withFileTypes: true });
        const fileList = await Promise.all(
          entries.map(async (entry) => {
            const fullPath = path.join(listPath, entry.name);
            const stats = await fs.promises.stat(fullPath);
            return {
              name: entry.name,
              type: entry.isDirectory() ? 'directory' : 'file',
              size: stats.size,
              modified: stats.mtime.toISOString(),
            };
          })
        );

        return res.json({
          path: subPath || '.',
          files: fileList,
        });
      }

      case 'read': {
        if (!filename || typeof filename !== 'string') {
          return res.status(400).json({ error: 'bad_request', message: 'filename is required for read action.' });
        }

        const sanitizedPath = sanitizePath(filename, baseDir);
        const fileEncoding = encoding === 'base64' ? 'base64' : 'text';
        const content = await readFileWithEncoding(sanitizedPath, fileEncoding);

        return res.json({
          filename,
          content,
          encoding: fileEncoding,
        });
      }

      default:
        return res.status(400).json({
          error: 'bad_request',
          message: `Unknown action: ${action}. Valid actions: save, delete, list, read.`,
        });
    }
  } catch (err) {
    const errorMessage = err instanceof Error ? err.message : String(err);
    console.error(`[files] Error: ${errorMessage}`);
    res.status(500).json({
      error: 'files_failed',
      message: errorMessage,
    });
  }
}

// Handler for multipart requests (large file uploads)
async function handleFilesMultipart(req: Request, res: Response) {
  const targetDir = (req.body.target_dir as string) || '';
  const uploadedFiles = req.files as Express.Multer.File[];

  if (!uploadedFiles || uploadedFiles.length === 0) {
    return res.status(400).json({ error: 'bad_request', message: 'No files uploaded.' });
  }

  const baseDir = LOCAL_ROOT;
  const savedFiles: string[] = [];
  const errors: string[] = [];

  for (const file of uploadedFiles) {
    try {
      const originalName = file.originalname;
      const destFilename = targetDir ? `${targetDir}/${originalName}` : originalName;

      const destPath = sanitizePath(destFilename, baseDir);
      await ensureDir(path.dirname(destPath));
      await fs.promises.rename(file.path, destPath);

      savedFiles.push(destFilename);
      console.log(`[files] Saved (multipart): ${destPath}`);
    } catch (err) {
      // Clean up temp file on error
      try {
        await fs.promises.unlink(file.path);
      } catch (_e) {
        // ignore cleanup errors
      }

      const errorMessage = err instanceof Error ? err.message : String(err);
      errors.push(`${file.originalname}: ${errorMessage}`);
      console.error(`[files] Error saving ${file.originalname}: ${errorMessage}`);
    }
  }

  if (errors.length > 0 && savedFiles.length === 0) {
    return res.status(500).json({
      error: 'upload_failed',
      message: 'All files failed to upload',
      errors,
    });
  }

  res.json({
    status: errors.length > 0 ? 'partial' : 'saved',
    files: savedFiles,
    errors: errors.length > 0 ? errors : undefined,
  });
}

// Route with content-type detection
app.post('/files', (req: Request, res: Response) => {
  const contentType = req.headers['content-type'] || '';

  if (contentType.includes('multipart/form-data')) {
    // Use multer middleware for multipart uploads
    uploadMiddleware.array('files', 100)(req, res, (err) => {
      if (err) {
        const message = err instanceof Error ? err.message : String(err);
        return res.status(400).json({ error: 'upload_error', message });
      }
      handleFilesMultipart(req, res);
    });
  } else {
    // JSON request
    handleFilesJson(req, res);
  }
});

app.get('/sessions', auth, async (_req: Request, res: Response) => {
  const sessions = Array.from(activeSessions.entries()).map(([sessionId, session]) => ({
    sessionId,
    mode: session.mode,
    createdAt: session.createdAt,
    lastAccessed: session.lastAccessed,
  }));
  res.json({ sessions });
});


function handleAgentError(err: unknown, res: Response, defaultErrorType = 'unknown') {
  if (err instanceof AgentError) {
    const agentErr = err as Error & { options: { variant: string; adaptable?: boolean } };
    console.error(`AgentError (${agentErr.options.variant}): ${agentErr.message}`);
    res.status(400).json({
      error: agentErr.options.variant,
      message: agentErr.message,
      adaptable: agentErr.options.adaptable
    });
  } else {
    let errorMessage = err instanceof Error ? err.message : String(err);
    if (errorMessage.includes('closed')) {
      const lifecycleHint = formatLastBrowserLifecycleHint();
      if (lifecycleHint) {
        console.error(`[browser-lifecycle] ${lifecycleHint}`);
        errorMessage = `${errorMessage} | ${lifecycleHint}`;
      }
    }
    console.error(`Unknown Error: ${errorMessage}`);
    res.status(500).json({
      error: defaultErrorType,
      message: errorMessage
    });
  }
}
