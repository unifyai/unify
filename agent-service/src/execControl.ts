import { ChildProcess } from 'child_process';

/**
 * Live registry of /exec child processes, keyed by execId.
 *
 * /exec is a blocking request: the caller does not hear back until the
 * command finishes. Registering the process under a caller-supplied id is
 * what lets a second request reach the run while it is still going — the
 * only channel through which a steering stop or pause can arrive from the
 * runtime. Entries remove themselves when the process closes, so a signal
 * for a finished run is a clean not_found rather than a stray kill.
 */
const running = new Map<string, ChildProcess>();

export type SignalAction = 'stop' | 'pause' | 'resume';
export type SignalOutcome = 'ok' | 'not_found' | 'unsupported';

export function registerExec(execId: string, proc: ChildProcess): void {
  running.set(execId, proc);
  proc.on('close', () => {
    running.delete(execId);
  });
}

export function runningExecCount(): number {
  return running.size;
}

/**
 * Deliver a steering action to a running exec's process group.
 *
 * POSIX children are spawned detached, so ``kill(-pid)`` reaches the whole
 * tree — a shell pipeline stops or freezes as one unit. A stop is SIGCONT +
 * SIGTERM (a frozen tree must thaw for the polite signal to act) with a
 * SIGKILL sweep after a grace period for anything that ignores it. Windows
 * has neither process groups nor stop signals: stop degrades to killing the
 * direct child, pause reports unsupported.
 */
export function signalExec(execId: string, action: SignalAction): SignalOutcome {
  const proc = running.get(execId);
  if (proc == null || proc.pid == null || proc.exitCode !== null) {
    return 'not_found';
  }

  if (process.platform === 'win32') {
    if (action !== 'stop') {
      return 'unsupported';
    }
    proc.kill();
    return 'ok';
  }

  const pgid = -proc.pid;
  try {
    if (action === 'pause') {
      process.kill(pgid, 'SIGSTOP');
    } else if (action === 'resume') {
      process.kill(pgid, 'SIGCONT');
    } else {
      process.kill(pgid, 'SIGCONT');
      process.kill(pgid, 'SIGTERM');
      setTimeout(() => {
        try {
          process.kill(pgid, 'SIGKILL');
        } catch {
          // Already gone.
        }
      }, 3000).unref();
    }
  } catch {
    return 'not_found';
  }
  return 'ok';
}
