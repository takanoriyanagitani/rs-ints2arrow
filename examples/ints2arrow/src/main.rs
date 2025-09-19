use std::io;
use std::process::ExitCode;

use rs_ints2arrow::arrow;

use arrow::array::Int16Array;

use rs_ints2arrow::sync::fs::filename2ints16be;

fn env2filename() -> Result<String, io::Error> {
    std::env::var("ENV_INPUT_RAW_INTS_16_BE").map_err(io::Error::other)
}

fn sub() -> Result<(), io::Error> {
    let ints_file_name: String = env2filename()?;
    let pa: Int16Array = filename2ints16be(ints_file_name)?;
    println!("len={}, content={pa:#?}", pa.len());
    Ok(())
}

fn main() -> ExitCode {
    match sub() {
        Ok(_) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}
