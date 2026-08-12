/**
 * Whether this deployment refuses the `/exec` command-execution endpoint.
 *
 * On the assistant runtime pod the agent-service shares a container with the
 * brain, so `/exec` would be arbitrary shell in the process that holds the
 * platform secrets, and its only auth is the pod's own UNIFY_KEY -- which
 * in-process sandbox code reads straight from its environment, so the check is
 * no boundary. The pod never calls /exec (the actor's local shell runs
 * in-process); only the remote desktop surfaces do, and they run this service
 * on their own machine and leave the flag unset. The pod sets
 * AGENT_SERVICE_DISABLE_EXEC to refuse it here without affecting them.
 */
export function isExecDisabled(value: string | undefined): boolean {
  return /^(1|true|yes|on)$/i.test((value || '').trim());
}
