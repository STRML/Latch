# Latch cluster queue: one queue across several Macs

Status: design for review (2026-11-09). Built on the STRML fork, offered upstream to CerebralCoding.

## Intent

Several Apple Silicon Macs that share work (today four Mac Studios on Thunderbolt 5) each run their own Latch. A job
that needs more than one machine, such as a tensor-parallel model window over four boxes, has no single place to wait.
Today it waits on one box's queue and grabs the other boxes with ad-hoc hold jobs over ssh, and nothing stops a build
queued on another box from landing in the middle of it. Agents also pick boxes by hand, and the operator's laptop
ended up running builds because nothing pointed work at an idle machine.

The cluster queue is one place where every job, single-box or multi-box, from any host, agent or laptop, waits its
turn across all the machines. A job says how many machines it needs and which ones it must have, and may name services
it is allowed to stop to make room. Latch picks idle machines, waits until each is quiet, stops what the job allows,
runs the job, and restores everything after.

## Goals

- One FIFO queue for the cluster, reachable from any member host and from a client machine that is not a member
  (the laptop), by CLI and by MCP.
- A job asks for a machine count plus optional required hosts. A tensor-parallel window asks for 4 machines with the
  first one pinned (it must include the host that owns the live server and the cable topology starts there). A build
  asks for 1 machine, any host.
- Latch chooses the hosts: required hosts, then idle members, in a stable order.
- Each member keeps its own admission exactly as today (sensors, quiet window, thermal guard, memory). The cluster
  queue decides who goes next and on which hosts; each host still decides when it is quiet enough.
- A job may name services it is allowed to stop (e.g. `local.mlx-serve`). Latch stops them before the job runs and
  always restores and health-checks them after, success or failure. Stopping mid-request is acceptable (the operator
  is the only user of these services and knows when cluster work runs).
- Jobs from different sessions and agents never land on a host that another cluster job holds.

## Non-goals

- Spawning the job's processes on remote hosts. The job runs on one lead host and drives the others itself (as the
  tensor-parallel window scripts do today); Latch tells it which hosts it got.
- Cross-host resource accounting beyond "this host is held by this job".
- Multi-user authentication or a network-facing service.
- Changing how a single-host Latch behaves when no cluster is configured.

## Anti-goals (a technically working result that is still a failure)

- A host gets a job while another cluster job holds it.
- A stopped service stays down after its job ends, fails or is cancelled.
- A queued cluster job holds partial hosts indefinitely, blocking everyone (holds are all-or-nothing in order).
- A listening socket on the LAN with its own auth surface.
- Single-host users see different behavior, new required config, or a slower path.

## Constraints (from the code today)

- Latch has no network code. The queue is `SchedulerState` JSON under a flock in `<lock>.queue/state.json`; liveness is
  local pids and flocks; the clock is `ProcessInfo.systemUptime`. None of this can be shared across machines.
- MCP submissions persist `MCPSubmission` in `DurableJobRecord` and use it for retry dedupe; adding fields changes a
  persisted contract. AGENTS.md: no persisted or wire contract change, version bump or schema bump without the
  maintainer's explicit approval for that specific change. This design needs that approval before it lands upstream.
- Swift Testing, `swift test -c release`, `swift format lint --strict`; Apple Silicon, macOS 26.

## The shape

```
any host / laptop ──submit──▶ coordinator (one member's Latch service, e.g. studio1)
                               cluster queue: FIFO of cluster jobs (persisted like DurableJobs)
                                 │ pick hosts (required + idle members, stable order)
                                 │ hold each host in ascending order:  ssh -tt <host> latch hold <tag>
                                 │   (an isolated ticket in that host's own Latch; admitted = host is ours)
                                 │ stop the job's allowed services (configured stop commands)
                                 │ run the job on the lead host, LATCH_HOSTS=<chosen hosts>
                                 │ restore services (start + health check), release holds
                                 ▼
             member hosts keep their local Latch: sensors, quiet window, thermal guard, memory
```

- **Membership** is a config file on the coordinator: each member's name, ssh destination, and the services it may
  stop (name, stop command, start command, health check, timeouts). A job can only name configured services.
- **Transport is ssh** (my call). Latch gains no listening socket; ssh brings authentication, works with keys the hosts
  already trust, and a held hold dies with its connection (`ssh -tt` hangs up the remote side), which gives free
  cleanup when a coordinator dies. Proven on the Studios: a queued hold is cancelled when its ssh drops.
- **Holds** become a first-class `latch hold <tag>`: an isolated (or measurement, matching the job) ticket that prints
  `HELD` when admitted and exits on release. Taken in ascending host order so two cluster jobs cannot deadlock.
  All-or-nothing: if a later host cannot be held within the job's admission timeout, earlier holds are released.
- **Routing (Sam's call): when a cluster is configured, every submission goes to the coordinator**, on members and on
  the laptop alike. A 1-machine job lands on an idle member through the same hold path, so nobody picks a box and
  nothing runs on the laptop. A machine with no cluster configured behaves exactly as today. The laptop gets a thin
  Latch install (CLI + MCP, no local scheduling of work) whose submissions, waits and reads go to the coordinator over
  ssh; job IDs, `requestKey` dedupe and results live on the coordinator. When the coordinator is unreachable, a
  submission fails with a clear error and an identical retry with the same `requestKey` is safe.
- **What a cluster job can do (Sam's call): a non-interactive subset first.** It runs a command on the lead host with
  stdout/stderr captured, an exit code, cancel and timeouts. No `input: pipe|terminal`, no `latch_input` /
  `latch_resize` / `latch_signal` beyond cancel, no checkpoints; a cluster submission that asks for those is refused
  with a reason. `executable` and `workingDirectory` must exist at the same paths on the lead host (true for the
  Studios: same user, same home layout); the submitter's environment is not forwarded, only `LATCH_HOSTS` and
  `LATCH_JOB_ID` are added. The job runs as a child of the lead's hold session, so cancel and a dropped coordinator end
  it; its exit code travels back on the session's own output (a final status line), not ssh's exit status, because
  Tailscale SSH drops exit statuses.
- **Services** are stopped only after every host is held, restored in a `finally`, then health-checked; a failed
  restore marks the job result with the failure and leaves the host's hold until the restore succeeds or an operator
  clears it.

## Policies (my calls, from the builder check)

- **Idle** is read from each member's own `latch view --json` over ssh: no local job running or queued, quiet.
- **Host set** is picked when the job reaches the head. Holds are taken in ascending host order with a per-job
  admission timeout (default 30 minutes for the whole set). If it expires, every hold taken is released, the job goes
  back to the head and the set is picked again; after three failed placements the job fails with the blocker named.
- **Holds reserve nothing** (no memory or CPU): the live model server is resident until a service stop frees it, and a
  memory reservation would trip the existing "unsatisfiable" stall before the stop can happen. The job's own memory
  is the operator's business, as the tensor-parallel scripts handle it today.
- **mayStop** acts on every held host that defines the service, is skipped on held hosts that don't, and a job naming
  a service no member defines is refused at submit.
- **Restore** starts only services that were up before the job (a health check before the stop decides), and the
  coordinator records every stop on disk before doing it, so a coordinator restart restores what a dead run stopped.
  A failed restore retries with backoff; the host stays held until it succeeds or `latch cluster clear <host>` is run,
  and the job's result says the restore failed.
- **ssh** for holds uses `ServerAliveInterval`, so a half-open connection releases its host.
- **Storage:** a cluster job lives in its own store under the coordinator's queue directory, not in `state.json`, so
  the coordinator's local admission does not gate it; `latch list` and `latch_jobs` show a merged view.

## Contracts (Sam's call)

The STRML fork adds optional fields to the MCP submission and the job records and a new cluster store; the schema
version stays 1 and nothing is removed. Upstream, these are persisted-contract changes that only the maintainer can
approve (AGENTS.md), so an issue goes to CerebralCoding first and a PR only after agreement.

## Interfaces (names are the builder's to finalize)

- CLI: `latch schedule --machines N [--host H ...] [--may-stop SERVICE ...] -- command`; `latch hold <tag>`;
  `latch cluster status` (each member: held by which job, local queue depth, reachable).
- MCP `latch_submit`: optional `machines` (int, default 1), `hosts` (array of member names), `mayStop` (array of
  service names). `latch_job`/`latch_jobs` summaries gain the assigned hosts and the services stopped/restored.
- The job's environment: `LATCH_HOSTS` (ordered, lead first) and `LATCH_JOB_ID`.

## Testing

- An injectable host transport (like the scheduler's injected `collect` closure) so the coordinator's pick / hold /
  stop / restore / release sequence is tested without ssh: holds that admit late, a host that never admits (partial
  holds released), a service whose restore fails, a cancel mid-run, two cluster jobs racing for overlapping hosts
  (ascending order, no deadlock), coordinator restart with jobs queued and running (running ones finish as
  "will not be replayed", as today).
- One real-ssh test path behind an opt-in environment variable.

## Rollout

1. Build on the STRML fork, deploy on the four Studios (studio1 coordinator), replace `cluster-hold.sh`,
   `latch-hold` and the hand-picked boxes in the operator's scripts.
2. Open an issue upstream describing the need and this shape before the PR, because it changes persisted contracts.
3. PR upstream once the maintainer agrees to the contract changes.

## Left to the builder

- Exact field, flag and file names; where cluster job records live under the coordinator's queue directory.
- Whether a cluster job's measurement flag maps to `measurement` holds on every host or only the lead.
- Backfill: strict FIFO first; letting a 1-machine job pass a blocked 4-machine job is a later option, not in scope.
