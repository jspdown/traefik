# wasihttp test plugin

A Rust component that targets the `traefik:plugin/middleware` world defined in
[`pkg/plugins/wit`](../../wit). The plugin tests run `plugin.wasm`. The `mode`
configuration option selects the behavior under test (see `src/lib.rs`).

## Build

`rust-toolchain.toml` pins the Rust version and the `wasm32-wasip2` target, and
rustup installs them on the first build. From this directory:

```bash
cargo build --release --target wasm32-wasip2
cp target/wasm32-wasip2/release/wasihttp.wasm plugin.wasm
```

Commit the new `plugin.wasm` with the change to the source.
