mod parser;

use clap::{arg, Command};
use std::path::PathBuf;

fn cli() -> Command {
    Command::new("mlg")
        .about("Command line tool for MLG files")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .allow_external_subcommands(true)
        .subcommand(
            Command::new("convert")
                .about("Converts MLG file to another format")
                .arg_required_else_help(true)
                .arg(arg!(<FORMAT> "Target format, one of: [csv, json]"))
                .arg(
                    arg!(<PATH> ... "Files to convert").value_parser(clap::value_parser!(PathBuf)),
                ),
        )
}

fn main() {
    use std::time::Instant;
    let now = Instant::now();

    match cli().get_matches().subcommand() {
        Some(("convert", sub_matches)) => {
            let format = sub_matches
                .get_one::<String>("FORMAT")
                .expect("required")
                .as_str();
            let paths = sub_matches
                .get_many::<PathBuf>("PATH")
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();

            match format {
                "csv" => {
                    parser::parse(paths, parser::Formats::Csv);
                }
                "json" => {
                    parser::parse(paths, parser::Formats::Json);
                }
                _ => {
                    println!("Invalid format: {}", format);
                }
            }

            let elapsed = now.elapsed();
            println!("Finished in: {:.2?}", elapsed);
        }
        _ => cli().print_help().unwrap(),
    }
}
