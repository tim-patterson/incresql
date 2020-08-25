use runtime::Runtime;
use std::error::Error;
use std::process::Command;

fn main() -> Result<(), Box<dyn Error>> {
    println!("Building dbgen");
    Command::new("docker")
        .args(&["build", "--tag", "dbgen", "benchmarking/docker/dbgen"])
        .spawn()
        .unwrap()
        .wait()
        .expect("Error building dbgen");

    println!("Running dbgen");
    let current_dir = std::env::current_dir().unwrap();
    let dbgendata_dir = current_dir.join("target").join("dbgen_s1");
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
            "/tpch-dbgen/dbgen",
            "-s",
            "1",
            "-f",
            "-b",
            "/tpch-dbgen/dists.dss",
        ])
        .spawn()
        .unwrap()
        .wait()
        .expect("Error running dbgen");

    Runtime::new_for_test();
    Ok(())
}
