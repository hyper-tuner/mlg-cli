# MLG CLI

Command line tool for MLG files.

## Download

See: [releases](https://github.com/hyper-tuner/mlg-cli/releases).

## Usage

```bash
Command line tool for MLG files

Usage: mlg <COMMAND>

Commands:
  convert  Converts MLG file to another format
  help     Print this message or the help of the given subcommand(s)

Options:
  -h, --help  Print help information
```

## Development

```bash
# dev
cargo-watch -x run # cargo install cargo-watch

# profile
sudo cargo flamegraph --dev

# bench
hyperfine --warmup 3 ./target/release/mlg
```
