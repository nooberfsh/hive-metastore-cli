# Hive Metastore Client & Server

## Build with linux musl ([stackoverflow](https://stackoverflow.com/a/53315626))
```
rustup target add x86_64-unknown-linux-musl
cargo build --release --target=x86_64-unknown-linux-musl
```