# Metrics

A node serves Prometheus text when it is started with `--metrics-bind`:

```bash
felixd agent --metrics-bind 127.0.0.1:9464 ...    # or FELIXD_METRICS_BIND
curl -s 127.0.0.1:9464/metrics
```

`/live` answers `ok` for a probe. If the address cannot be bound the agent
exits at start. At most 64 connections are served at once, and a client has
5 seconds to send its request and to take the reply. Nothing is served and nothing is counted
unless the flag is given. When it is, the counters are atomics that are read
only when a scrape arrives, and nothing in felixd reads them back, so they
cannot change what a node does. The endpoint is plain HTTP with no
authentication, and it names workloads and streams. Keep it on a network only
Prometheus and operators reach.

Each node reports itself. There is no cluster-wide series: sum or compare
across scrape targets, and use `felixd ps` for the view from the log, which
agrees with every node. The names follow Felix's: the `felixd_` prefix,
`_total` on counters, `_seconds` for time.

```yaml
scrape_configs:
  - job_name: felixd
    static_configs:
      - targets: ['node-1:9464', 'node-2:9464', 'node-3:9464']
```

## Per node

| Metric | Type | Meaning |
|---|---|---|
| `felixd_node_info{node,version}` | gauge | always 1 |
| `felixd_node_draining` | gauge | 1 while the node is marked draining and claims nothing |
| `felixd_leases_held` | gauge | workloads the node holds the lease of and runs |
| `felixd_lease_claims_total{result}` | counter | `won`, `contended` (another node's lease was there) or `error` |
| `felixd_lease_handoffs_total{reason}` | counter | leases the node let go of for another to take: `drained`, or `lost` when it was not renewed in time or was taken |
| `felixd_needs_operator_total{reason}` | counter | workloads the node stopped for an operator, by the reason code `ps` shows, as `module_refused`, `module_missing`, `spec_unrecorded`, `spec_conflict`, `spec_stale`, `restore_fuel`, `input_unreadable`, `input_behind`, `history_trimmed` |
| `felixd_workload_failures_total` | counter | workloads the node marked failed |
| `felixd_snapshots_total{result}` | counter | `written`, or `skipped` when a snapshot ran out of `snapshot_fuel` |
| `felixd_snapshot_duration_seconds` | histogram | taking and writing a snapshot, all workloads |

## Per workload

Labelled `workload`. Counters keep their value after the node stops holding
the workload, and gauges go to 0, so a rate over a handoff shows the old
holder going quiet and the new one starting.

| Metric | Type | Meaning |
|---|---|---|
| `felixd_workload_held` | gauge | 1 while this node holds the lease |
| `felixd_steps_total` | counter | inputs stepped on this node |
| `felixd_step_duration_seconds` | histogram | time in the workload's `step` call, 0.1 ms to 5 s |
| `felixd_output_records_total` | counter | output records this node appended |
| `felixd_output_refused_total{result}` | counter | appends the output stream refused because its tail had moved. `reconciled`: the holder read the stream and found its own earlier write (a lost answer) or a race, and carried on. `fenced`: it found a later holder's records and stopped. A few `reconciled` are normal; any `fenced` means a holder was superseded |
| `felixd_merge_log_appends_total` | counter | entries appended to the merge log of a workload with several inputs |
| `felixd_workload_restarts_total` | counter | times its run was restarted after an error talking to Felix |
| `felixd_workload_snapshot_bytes` | gauge | size of its last snapshot, chunks together |
| `felixd_input_lag_records{input}` | gauge | records on the input past the next one the workload steps |

Input lag is the input's tail, read every 5 seconds while the node holds the
workload, minus the next offset the workload steps. It is at most 5 seconds
stale and shows nothing until the first read. It counts records on the
stream, claims included when the input is another workload's output, as
`felixd ps` does. It is not shown for a workload the node does not hold, and it is dropped after three failed reads in a row. Reads are spread over 5 to 6 seconds.

## Cardinality

A node keeps series for the workloads it has held or claimed, at most
`--metrics-max-workloads` of them (default 100). Past that, a workload it no
longer holds is dropped to make room, and its counters start over if it
comes back. Each costs about 25 series,
mostly the step histogram, and one more per input, so the default is a few
thousand series per node at most. Past the limit a node reports all further
workloads together as `workload="_other"`, summed, with no input lag or
snapshot size, only while every named workload is held. Raise
the limit if Prometheus can take it, or look at the workloads that matter by
name with `felixd ps`. Nothing else is labelled by a name: the other labels
(`result`, `reason`, `le`) have fixed values, and `input` is bounded by the
workload's inputs.

## OpenTelemetry

felixd does not push OTLP. An OpenTelemetry Collector scrapes the endpoint
with its Prometheus receiver and exports it from there; the metric names and
labels above are what it sees. Traces, replay time and per-effect latency are
not built; effects are not either (see
[design.md](design.md#operating-a-cluster)).

## Questions the metrics answer

- *Is a workload keeping up?* `felixd_input_lag_records` falling or flat near
  0, and `rate(felixd_steps_total[1m])` against the input rate.
- *Is a step slow?* `histogram_quantile(0.99, rate(felixd_step_duration_seconds_bucket[5m]))`.
- *Is a workload moving between nodes?* `rate(felixd_lease_claims_total{result="won"}[10m])`
  above 0 with no drain, or `felixd_lease_handoffs_total{reason="lost"}`:
  leases are not renewed in time, which is a slow node or broker.
- *Did a stale holder try to write?* `felixd_output_refused_total`.
- *Is something stuck?* `felixd_needs_operator_total` by reason, then
  `felixd ps` for the workload.
- *Is a drain finished?* `felixd_node_draining` is 1 and `felixd_leases_held`
  is 0.
