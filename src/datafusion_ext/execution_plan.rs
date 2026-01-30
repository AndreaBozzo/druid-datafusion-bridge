use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::error::Result as DFResult;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};

use crate::segment::DruidSegment;

/// An ExecutionPlan that reads data from a Druid segment.
///
/// Supports projection pushdown: only the columns requested by DataFusion
/// are read from the segment, avoiding IO for unused columns.
#[derive(Debug)]
pub struct DruidSegmentExec {
    segment: Arc<DruidSegment>,
    projection: Option<Vec<usize>>,
    projected_schema: SchemaRef,
    properties: PlanProperties,
}

impl DruidSegmentExec {
    pub fn new(segment: Arc<DruidSegment>, projection: Option<Vec<usize>>) -> Self {
        let projected_schema = match &projection {
            Some(indices) => {
                let schema = segment.schema();
                let fields: Vec<Field> = indices.iter().map(|&i| schema.field(i).clone()).collect();
                Arc::new(Schema::new(fields))
            }
            None => segment.schema(),
        };

        let properties = PlanProperties::new(
            EquivalenceProperties::new(projected_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        );

        Self {
            segment,
            projection,
            projected_schema,
            properties,
        }
    }
}

impl DisplayAs for DruidSegmentExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DruidSegmentExec: projection={:?}", self.projection)
    }
}

impl ExecutionPlan for DruidSegmentExec {
    fn name(&self) -> &str {
        "DruidSegmentExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let batch = match &self.projection {
            Some(indices) => {
                let schema = self.segment.schema();
                let col_names: Vec<&str> = indices
                    .iter()
                    .map(|&i| schema.field(i).name().as_str())
                    .collect();
                self.segment
                    .read_columns(&col_names)
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
            }
            None => self
                .segment
                .read_all()
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?,
        };

        Ok(Box::pin(MemoryStream::try_new(
            vec![batch],
            self.projected_schema.clone(),
            None,
        )?))
    }
}
