//! Small CLI that reads CSV/JSON logs and writes the rollup as CSV/Parquet.
use anyhow::Result;
use clap::{Parser, ValueEnum};
use polars::prelude::*;
use rusty_cruncher::rollup_on_files;

#[derive(Copy, Clone, PartialEq, Eq, ValueEnum)]
enum OutFmt {
    Csv,
    Parquet,
}

#[derive(Parser, Debug)]
#[command(
    name = "rusty-cruncher",
    version,
    about = "Fast rolling/grouped stats over logs"
)]
struct Args {
    /// One or more CSV/JSON files (glob expansion is done by your shell)
    #[arg(required = true)]
    input: Vec<String>,
    /// Group-by keys, e.g. user_id,service
    #[arg(long, value_delimiter = ',')]
    keys: Vec<String>,
    /// Numeric column to compute rolling stats over
    #[arg(long)]
    value: String,
    /// Row-based window size (e.g. 128)
    #[arg(long, default_value_t = 128)]
    window: usize,
    /// Z-score threshold to flag anomalies
    #[arg(long, default_value_t = 3.0)]
    z: f64,
    /// Output file path (extension may be changed by --out-fmt)
    #[arg(long, default_value = "rollup.csv")]
    out: String,
    /// Output format (csv|parquet)
    #[arg(long, value_enum, default_value_t = OutFmt::Csv)]
    out_fmt: OutFmt,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let df = rollup_on_files(&args.input, &args.keys, &args.value, args.window, args.z)?;

    match args.out_fmt {
        OutFmt::Csv => {
            CsvWriter::new(std::fs::File::create(&args.out)?).finish(&df)?;
        }
        OutFmt::Parquet => {
            ParquetWriter::new(std::fs::File::create(&args.out)?).finish(&df)?;
        }
    }

    eprintln!("wrote {} rows to {}", df.height(), args.out);
    Ok(())
}
