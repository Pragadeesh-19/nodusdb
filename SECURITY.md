# Security policy

NodusDB answers authorization checks, signs the log it ships and erases people on request, so a bug in it can let
someone see what they should not, or keep data that should be gone. We take those reports first.

## Reporting a vulnerability

Report it privately through GitHub: open the repository's **Security** tab and choose **Report a vulnerability**. Only
the maintainers see the report. Please do not open a public issue or pull request for it.

Useful things to include:

- The nodusdb version (`python -c "import nodusdb; print(nodusdb.__version__)"`) and your platform.
- What an attacker can do, and what they need first: a writer's process, access to the bucket, a follower's
  configuration, or nothing.
- A script or a sequence of steps that shows it, as short as you can make it.

We will acknowledge the report within three working days, tell you what we found within ten, and agree a disclosure
date with you before anything is published. We credit reporters in the release notes unless you would rather we did
not.

## What counts

These are in scope:

- A check that grants access the tuples and the schema do not allow, or that ignores an `at_least` token.
- A follower, `restore`, takeover or `as_of` that accepts a chain object that is unsigned, signed by an unknown key,
  out of order or forked, or that takes the public key from the bucket.
- A name that survives `wait_erased` in any copy NodusDB manages: memory, the graph directory, the chain and
  snapshots in the bucket, or the Iceberg table. [docs/operations.md](docs/operations.md) lists what erasure cannot
  reach, such as bucket versioning or copies you made yourself.
- Memory corruption, or a crash of the host process, caused by input passed through the public API.
- A way to read or change another graph's data through the C interface's handles.

These are not:

- Anything that needs control of the writer's process or its signing key.
- Slow checks or large memory use from a graph you chose to load. `max_memory_mb` caps the native memory.
- Benchmark disagreements. Open an issue on [AuthzBench](https://github.com/Pragadeesh-19/authzbench) instead.

## Supported versions

Security fixes go into the latest 0.1.x release. Upgrade to it; a directory written by any 0.1.x release opens in
every later one.
