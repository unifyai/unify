interface BrowserConnection {
  isConnected(): boolean;
}

interface BrowserContextLifecycle {
  browser(): BrowserConnection | null;
  once(event: "close", listener: () => void): unknown;
  off(event: "close", listener: () => void): unknown;
}

interface BrowserBackedSession {
  agent: {
    context: BrowserContextLifecycle;
    page: { isClosed(): boolean };
  };
}

export class BrowserSessionUnavailableError extends Error {
  constructor(sessionId: string) {
    super(`Browser disconnected while session ${sessionId} was starting.`);
    this.name = "BrowserSessionUnavailableError";
  }
}

function isLive(session: BrowserBackedSession): boolean {
  return session.agent.context.browser()?.isConnected() === true
    && !session.agent.page.isClosed();
}

export function registerLiveSession<T extends BrowserBackedSession>(
  sessions: Map<string, T>,
  sessionId: string,
  session: T,
  onUnexpectedClose: () => void,
): void {
  let closed = false;
  const handleClose = () => {
    closed = true;
    if (sessions.get(sessionId) !== session) return;
    sessions.delete(sessionId);
    onUnexpectedClose();
  };

  session.agent.context.once("close", handleClose);
  if (!isLive(session)) {
    session.agent.context.off("close", handleClose);
    throw new BrowserSessionUnavailableError(sessionId);
  }

  sessions.set(sessionId, session);
  if (closed || !isLive(session)) {
    sessions.delete(sessionId);
    session.agent.context.off("close", handleClose);
    throw new BrowserSessionUnavailableError(sessionId);
  }
}

export function unregisterSession<T>(
  sessions: Map<string, T>,
  sessionId: string,
): T | undefined {
  const session = sessions.get(sessionId);
  sessions.delete(sessionId);
  return session;
}
