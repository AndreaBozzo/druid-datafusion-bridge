use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use clap::{Parser, Subcommand, ValueEnum};
use datafusion::prelude::SessionContext;

use druid_datafusion_bridge::datafusion_ext::table_provider::DruidSegmentTable;
use druid_datafusion_bridge::segment::DruidSegment;

#[derive(Parser)]
#[command(
    name = "druid-segment",
    about = "Explore Apache Druid segment files and query them with SQL"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Show segment metadata: columns, types, interval, row count
    Info {
        /// Path to the segment directory
        #[arg(value_name = "SEGMENT_DIR")]
        path: PathBuf,
    },

    /// List all logical files in the smoosh archive
    Files {
        /// Path to the segment directory
        #[arg(value_name = "SEGMENT_DIR")]
        path: PathBuf,
    },

    /// Print rows from the segment
    Dump {
        /// Path to the segment directory
        #[arg(value_name = "SEGMENT_DIR")]
        path: PathBuf,

        /// Columns to include (default: all)
        #[arg(short, long)]
        columns: Option<Vec<String>>,

        /// Maximum rows to print
        #[arg(short, long, default_value = "20")]
        limit: usize,

        /// Output format
        #[arg(short, long, default_value = "table")]
        format: OutputFormat,
    },

    /// Run a SQL query against a segment using DataFusion
    Query {
        /// Path to the segment directory
        #[arg(value_name = "SEGMENT_DIR")]
        path: PathBuf,

        /// SQL query to execute (table name is 'segment')
        #[arg(short, long)]
        sql: String,
    },
}

#[derive(Clone, ValueEnum)]
enum OutputFormat {
    Table,
    Json,
    Csv,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Info { path } => cmd_info(&path)?,
        Commands::Files { path } => cmd_files(&path)?,
        Commands::Dump {
            path,
            columns,
            limit,
            format,
        } => cmd_dump(&path, columns.as_deref(), limit, &format)?,
        Commands::Query { path, sql } => cmd_query(&path, &sql).await?,
    }

    Ok(())
}

fn cmd_info(path: &Path) -> Result<()> {
    let segment = DruidSegment::open(path)?;
    let metadata = segment.metadata();
    let schema = segment.schema();

    println!("Segment: {}", path.display());
    println!(
        "Interval: {} .. {}",
        format_millis(metadata.interval_start_ms),
        format_millis(metadata.interval_end_ms)
    );
    println!("Columns ({}):", metadata.columns.len());
    for field in schema.fields() {
        println!("  {}: {}", field.name(), field.data_type());
    }
    println!("Dimensions: {}", metadata.dimensions.join(", "));

    match segment.num_rows() {
        Ok(rows) => println!("Rows: {}", rows),
        Err(e) => println!("Rows: (error reading: {})", e),
    }

    Ok(())
}

fn cmd_files(path: &Path) -> Result<()> {
    let segment = DruidSegment::open(path)?;
    let smoosh = segment.smoosh();

    println!("Logical files in smoosh archive:");
    for entry in smoosh.entries() {
        println!(
            "  {:40} chunk={} offset={}..{} ({} bytes)",
            entry.name,
            entry.chunk_number,
            entry.start_offset,
            entry.end_offset,
            entry.size()
        );
    }
    println!("Total: {} files", smoosh.len());

    Ok(())
}

fn cmd_dump(
    path: &Path,
    columns: Option<&[String]>,
    limit: usize,
    format: &OutputFormat,
) -> Result<()> {
    let segment = DruidSegment::open(path)?;

    let batch = match columns {
        Some(cols) => {
            let col_refs: Vec<&str> = cols.iter().map(|s| s.as_str()).collect();
            segment.read_columns(&col_refs)?
        }
        None => segment.read_all()?,
    };

    // Apply row limit
    let batch = if batch.num_rows() > limit {
        batch.slice(0, limit)
    } else {
        batch
    };

    match format {
        OutputFormat::Table => {
            let formatted = arrow::util::pretty::pretty_format_batches(&[batch])?;
            println!("{}", formatted);
        }
        OutputFormat::Json => {
            let mut writer = arrow::json::LineDelimitedWriter::new(std::io::stdout());
            writer.write(&batch)?;
            writer.finish()?;
        }
        OutputFormat::Csv => {
            let mut writer = arrow::csv::WriterBuilder::new()
                .with_header(true)
                .build(std::io::stdout());
            writer.write(&batch)?;
        }
    }

    Ok(())
}

async fn cmd_query(path: &Path, sql: &str) -> Result<()> {
    let table = DruidSegmentTable::open(path)?;
    let ctx = SessionContext::new();
    ctx.register_table("segment", Arc::new(table))?;

    let df = ctx.sql(sql).await?;
    df.show().await?;

    Ok(())
}

/// Format epoch millis as a human-readable datetime string.
fn format_millis(millis: i64) -> String {
    // Simple formatting without chrono dependency
    let secs = millis / 1000;
    let ms = millis % 1000;
    let days_since_epoch = secs / 86400;
    let time_of_day = secs % 86400;
    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;

    // Simple date calc from days since epoch (1970-01-01)
    let (year, month, day) = days_to_ymd(days_since_epoch);
    format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:03} UTC",
        year, month, day, hours, minutes, seconds, ms
    )
}

/// Convert days since Unix epoch to (year, month, day).
fn days_to_ymd(days: i64) -> (i64, u32, u32) {
    // Civil calendar algorithm from http://howardhinnant.github.io/date_algorithms.html
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if m <= 2 { y + 1 } else { y };
    (year, m, d)
}
