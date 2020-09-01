mod incresql_runner;
mod mysql_runner;
use std::error::Error;
use std::process::Command;

use clap::{App, Arg};
use incresql_runner::IncresqlRunner;
use mysql_runner::MysqlRunner;
use std::path::Path;

pub fn main() -> Result<(), Box<dyn Error>> {
    let matches = App::new("TPCH")
        .arg(
            Arg::with_name("scale")
                .short("s")
                .long("scale")
                .default_value("1")
                .possible_values(&["1", "5", "10", "15"]),
        )
        .arg(
            Arg::with_name("skip_load")
                .long("skipload")
                .takes_value(false),
        )
        .arg(Arg::with_name("mysql").long("mysql").takes_value(false))
        .get_matches();

    let s = matches.value_of("scale").unwrap().parse().unwrap();
    let skip_load = matches.is_present("skip_load");

    let current_dir = std::env::current_dir().unwrap();
    let dbgendata_dir = current_dir.join("target").join(format!("dbgen_s{}", s));

    if !skip_load {
        build_test_data(s, dbgendata_dir.as_path())?;
    }

    let mut runner: Box<dyn BenchmarkRunner> = if matches.is_present("mysql") {
        Box::from(MysqlRunner::new(s as u8)?)
    } else {
        Box::from(IncresqlRunner::new(s as u8, !skip_load)?)
    };
    if !skip_load {
        runner.create_tables()?;
        runner.load_tables(dbgendata_dir.as_os_str().to_str().unwrap())?
    }
    runner.run_queries()?;
    eprintln!("Done");
    Ok(())
}

fn build_test_data(s: u32, dbgendata_dir: &Path) -> Result<(), Box<dyn Error>> {
    if dbgendata_dir.is_dir() {
        eprintln!("dbgen files already exist :)");
        return Ok(());
    }

    eprintln!("Building dbgen");
    Command::new("docker")
        .args(&["build", "--tag", "dbgen", "benchmarking/docker/dbgen"])
        .spawn()?
        .wait()?;

    eprintln!("Running dbgen");
    std::fs::create_dir_all(&dbgendata_dir).unwrap();
    let datadir_str = dbgendata_dir.as_os_str().to_str().unwrap();
    Command::new("docker")
        .args(&[
            "run",
            "-w",
            "/data",
            "-v",
            &format!("{}:/data", datadir_str),
            "dbgen",
            "/_tpch-dbgen/dbgen",
            "-s",
            &s.to_string(),
            "-f",
            "-b",
            "/_tpch-dbgen/dists.dss",
        ])
        .spawn()?
        .wait()
        .expect("Error running dbgen");
    Ok(())
}

pub trait BenchmarkRunner {
    fn create_tables(&mut self) -> Result<(), Box<dyn Error>>;
    fn load_tables(&mut self, data_dir: &str) -> Result<(), Box<dyn Error>>;
    fn run_queries(&mut self) -> Result<(), Box<dyn Error>>;
}
