use std::any::Any;
use std::path::Path;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;

use super::execution_plan::DruidSegmentExec;
use crate::error::Result;
use crate::segment::DruidSegment;

/// A DataFusion TableProvider backed by a Druid segment directory.
///
/// Enables SQL queries on Druid segments:
/// ```ignore
/// let ctx = SessionContext::new();
/// let table = DruidSegmentTable::open(Path::new("/path/to/segment"))?;
/// ctx.register_table("my_datasource", Arc::new(table))?;
/// let df = ctx.sql("SELECT * FROM my_datasource LIMIT 10").await?;
/// ```
#[derive(Debug)]
pub struct DruidSegmentTable {
    segment: Arc<DruidSegment>,
}

impl DruidSegmentTable {
    /// Create from an already-opened segment.
    pub fn new(segment: DruidSegment) -> Self {
        Self {
            segment: Arc::new(segment),
        }
    }

    /// Open a segment directory and create a table provider.
    pub fn open(path: &Path) -> Result<Self> {
        let segment = DruidSegment::open(path)?;
        Ok(Self::new(segment))
    }
}

#[async_trait]
impl TableProvider for DruidSegmentTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.segment.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DruidSegmentExec::new(
            self.segment.clone(),
            projection.cloned(),
        )))
    }
}
