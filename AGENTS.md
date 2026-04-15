# Coding Agent Instructions for RisingWave

## Project Overview

RisingWave is a Postgres-compatible streaming database that offers the simplest and most cost-effective way to process, analyze, and manage real-time event data — with built-in support for the Apache Iceberg™ open table format.

RisingWave components are developed in Rust and split into several crates:

1. `config` - Default configurations for servers
2. `prost` - Generated protobuf rust code (grpc and message definitions)
3. `stream` - Stream compute engine
4. `batch` - Batch compute engine for queries against materialized views
5. `frontend` - SQL query planner and scheduler
6. `storage` - Cloud native storage engine
7. `meta` - Meta engine
8. `utils` - Independent util crates
9. `cmd` - All binaries, `cmd_all` contains the all-in-one binary `risingwave`
10. `risedevtool` - Developer tool for RisingWave

## Coding Style

- Always write code comments in English.
- Write simple, easy-to-read and easy-to-maintain code.
- Use `cargo fmt` to format the code if needed.
- Follow existing code patterns and conventions in the repository.

## Build, Run, Test

You may need to learn how to build and test RisingWave when implementing features or fixing bugs.

### Build & Check

- Use `./risedev b` to build the project.
- Use `./risedev c` to check if the code follow rust-clippy rules, coding styles, etc.
- After changing config definitions or defaults under `src/common/src/config`, run `./risedev generate-example-config` to update `src/config/example.toml` and `src/config/docs.md`.

### Unit Test

- Integration tests and unit tests are valid Rust/Tokio tests, you can locate and run those related in a standard Rust way.
- Parser tests: use `./risedev update-parser-test` to regenerate expected output.
- Planner tests: use `./risedev run-planner-test [name]` to run and `./risedev do-apply-planner-test` (or `./risedev dapt`) to update expected output.

### End-to-End Test

- Use `./risedev d` to run a RisingWave instance in the background via tmux.
  - It builds RisingWave binaries if necessary. The build process can take up to 10 minutes, depending on the incremental build results. Use a large timeout for this step, and be patient.
  - It kills the previous instance, if exists.
  - Optionally pass a profile name (see `risedev.yml`) to choose external services or components. By default it uses `default`.
  - This runs in the background so you do not need a separate terminal.
  - You can connect right after the command finishes.
  - Logs are written to files in `.risingwave/log` folder.
- Use `./risedev k` to stop a RisingWave instance started by `./risedev d`.
- Only when a RisingWave instance is running, you can use `./risedev psql -c "<your query>"` to run SQL queries in RW.
- Only when a RisingWave instance is running, you can use `./risedev slt './path/to/e2e-test-file.slt'` to run end-to-end SLT tests.
  - File globs like `/**/*.slt` is allowed.
  - Failed run may leave some objects in the database that interfere with next run. Use `./risedev slt-clean ./path/to/e2e-test-file.slt` to reset the database before running tests.
- The preferred way to write tests is to write tests in SQLLogicTest format.
- Tests are put in `./e2e_test` folder.

### Sandbox escalation

When sandboxing is enabled, these commands need `require_escalated` because they bind or connect to local TCP sockets:

- `./risedev d` and `./risedev p` (uses local ports and tmux sockets)
- `./risedev psql ...` or direct `psql -h localhost -p 4566 ...` (local TCP connection)
- `./risedev slt './path/to/e2e-test-file.slt'` (connects to local TCP via psql protocol)
- Any command that checks running services via local TCP (for example, health checks or custom SQL clients)

## Docker Image Build

The production Docker image is built via `docker/Dockerfile`. It compiles RisingWave from source inside the container using a multi-stage build.

### Required build arg

`CARGO_PROFILE` must always be passed — the Dockerfile does **not** set a default:

```bash
docker build -f docker/Dockerfile \
  --build-arg CARGO_PROFILE=release \
  -t risingwave/risingwave:solace-connector .
```

Omitting `--build-arg CARGO_PROFILE=release` causes cargo to fail immediately with:
> `error: a value is required for '--profile <PROFILE-NAME>' but none was supplied`

### GitHub Actions workflow (fork)

The fork has `.github/workflows/build-docker-image-fork.yml` which builds and pushes to `ghcr.io/jessemenning/risingwave`. To trigger manually:

```bash
gh workflow run "build-docker-image-fork.yml" --repo jessemenning/risingwave --ref feat/solace-source-connector
```

**Important:** Always push to the `fork` remote (not `origin`, which points to upstream `risingwavelabs/risingwave`):

```bash
git push fork feat/solace-source-connector
```

The workflow pushes to the `:solace-connector` tag by default — that is what the FAA project's `docker-compose.yml` pulls. Do not change `DEFAULT_LABEL` to anything else.

Note: `gh workflow list` shows upstream workflows when run from this directory — always pass `--repo jessemenning/risingwave` explicitly.

### sccache (GHA backend)

sccache wraps `rustc` inside the container for crate-level caching across builds. It requires `ACTIONS_RUNTIME_TOKEN` passed as a BuildKit secret. The Dockerfile includes a graceful fallback: if the token is absent (local builds), `RUSTC_WRAPPER` is unset so the build proceeds uncached rather than failing. Cold builds take ~30–45 minutes.

### Solace C SDK version

The Solace C SDK (`solclient`) is **7.33.2.3**. OpenSSL 3.0.8 is statically embedded inside `libsolclient.a` — no separate ssl/crypto link targets exist. The linker only needs `static=solclient`. No `SOLACE_USE_SYSTEM_SSL` workaround is needed or present.

### solace-rs API extensions

The connector (`src/connector/src/source/solace/`) depends on methods not in upstream `solace-rs`. They live in `jessemenning/solace-rs` on `main`, which is checked out during the GHA build. If a new connector feature needs a missing C SDK binding, add it to `solace-rs/src/message/inbound.rs` (or `async_support.rs`) and push to that repo before triggering the Docker build.

Methods added beyond upstream: `is_redelivered`, `get_replication_group_message_id`, `get_payload_as_string`, `get_rcv_timestamp` (on `InboundMessage`); `client_name` (on `AsyncSessionBuilder`).

### Local build configuration

Active settings in `risedev-components.user.env`:
- `ENABLE_BUILD_RUST=true`
- `ENABLE_DYNAMIC_LINKING=true` — uses system shared libraries instead of bundled static deps; requires these system packages: `libzstd-dev libssl-dev liblz4-dev libsnappy-dev`

Active settings in `.cargo/config.toml`:
- `jobs = 4` — limits parallelism to avoid OOM on this machine

## Connector Development

See `docs/dev/src/connector/intro.md`.

## Repo Skills

- Use `$risingwave-rust-analyzer` for RisingWave-specific `rust-analyzer` CLI and LSP workflows, especially when a task needs fast semantic inspection, structural search/replace, or crate-specific feature guidance. Skill path: `.agents/skills/risingwave-rust-analyzer/SKILL.md`.
- Use `$fix-buildkite-ci` for Buildkite triage and focused CI fixes. Skill path: `.agents/skills/fix-buildkite-ci/SKILL.md`.
