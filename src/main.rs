use clap::{AppSettings, arg, Command};
use log::{LevelFilter};
use simplelog::*;
use razel::tasks::csv_concat;

fn main() {
    TermLogger::init(
        LevelFilter::Info,
        Config::default(),
        TerminalMode::Mixed,
        ColorChoice::Auto,
    )
        .unwrap();

    let matches = Command::new("razel")
        .arg_required_else_help(true)
        .setting(AppSettings::DeriveDisplayOrder)
        .subcommand(
            Command::new("command")
                .about("execute a custom command")
        )
        .subcommand(
            Command::new("script")
                .about("execute commands in batch file")
        )
        .subcommand(
            Command::new("task")
                .about("execute a single task")
                .subcommand(
                    Command::new("csv-concat")
                        .args(&[
                            arg!(<input> ... "input csv files"),
                            arg!(<output> "concatenated file to create"),
                        ])
                ))
        .get_matches();

    if let Some(matches) = matches.subcommand_matches("task") {
        if let Some(matches) = matches.subcommand_matches("csv-concat") {
            csv_concat(matches.values_of("input").unwrap().collect(), matches.value_of("output").unwrap()).unwrap();
        }
    }
}
