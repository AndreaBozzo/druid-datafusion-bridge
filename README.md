# druid-datafusion-bridge

> **⚠️ WORK IN PROGRESS**: This library is in active development and is not yet ready for production use. API and functionality are subject to change.

A Rust library bridging Apache Druid segment files with Apache DataFusion.

## Features

- **Direct Segment Access**: Reads Druid `index.dr` (smoosh) files directly without a running Druid cluster.
- **DataFusion Integration**: Implements DataFusion `TableProvider` and `ExecutionPlan` traits for querying segments.
- **Column Support**:
  - Primitives: Strings, Longs, Floats, Doubles
  - Encodings: LZ4/LZO compression, Bitmaps (Roaring/Concise), FrontCoded, Dictionary encoding
  - Complex types: HyperLogLog (partial), ApproxHistogram (partial)
- **Vectorized Execution**: Zero-copy (where possible) mapping to Arrow RecordBatches.

## Usage

```rust
use druid_datafusion_bridge::datafusion_ext::table_provider::DruidTableProvider;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    let provider = DruidTableProvider::try_new("path/to/segment/index.dr")?;
    
    ctx.register_table("druid_table", Arc::new(provider))?;
    
    let df = ctx.sql("SELECT * FROM druid_table LIMIT 10").await?;
    df.show().await?;
    
    Ok(())
}
```

## Architecture

- **`src/segment`**: Parsing logic for Druid segment metadata (`metadata.dr`, `version.bin`) and smoosh file handling.
- **`src/column`**: Decoders for Druid's column formats (VSize Ints, Compressed Columnar, etc).
- **`src/datafusion_ext`**: DataFusion adapter layer.
