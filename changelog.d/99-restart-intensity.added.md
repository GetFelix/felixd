- `felixd run --max-takeovers N --takeover-window-ms MS` sets a restart intensity in the spec (#99).
  Every holder counts the takeovers in a row with no output between them from the claim records on
  `wl.<id>.out`, so every node agrees on the count. Past the limit the workload is recorded as
  needing an operator, reason 11 `restart intensity exceeded`, fenced by epoch, until a new spec
  record. Unset specs encode and run as before.
