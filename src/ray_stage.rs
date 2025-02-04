use std::{fmt::Formatter, sync::Arc};

use datafusion::error::Result;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion::{arrow::datatypes::SchemaRef, execution::SendableRecordBatchStream};

/// An execution plan that serves as a marker of where we want to split the physical plan into
/// stages.
///
/// This marker is consumed later by the [`crate::dataframe::RayDataFrame`], when we are told to execute.   It will
/// create the discrete stages and insert other ExecutionPlans to read and write the data
///
/// # Example
/// The following query,
/// ```sql
/// select c.c_name, sum(o.o_totalprice) as total
/// from orders o inner join customer c on o.o_c ustkey = c.c_custkey
/// group by c_name limit 1
/// ```
///
/// Will produce the following physical_plan from the optimizer
///
/// ```
/// RayStageExec[3] (output_partitioning=UnknownPartitioning(1))
///  ProjectionExec: expr=[c_name@0 as c_name, sum(o.o_totalprice)@1 as total]
///    GlobalLimitExec: skip=0, fetch=1
///      CoalescePartitionsExec
///        AggregateExec: mode=FinalPartitioned, gby=[c_name@0 as c_name], aggr=[sum(o.o_totalprice)]
///          RayStageExec[2] (output_partitioning=Hash([Column { name: "c_name", index: 0 }], 2))
///            RepartitionExec: partitioning=Hash([c_name@0], 2), input_partitions=2
///              AggregateExec: mode=Partial, gby=[c_name@1 as c_name], aggr=[sum(o.o_totalprice)]
///                ProjectionExec: expr=[o_totalprice@1 as o_totalprice, c_name@0 as c_name]
///                  HashJoinExec: mode=Partitioned, join_type=Inner, on=[(c_custkey@0, o_custkey@0)], projection=[c_name@1, o_totalprice@3]
///                    RayStageExec[0] (output_partitioning=Hash([Column { name: "c_custkey", index: 0 }], 2))
///                      RepartitionExec: partitioning=Hash([c_custkey@0], 2), input_partitions=1
///                        ParquetExec: file_groups={1 group: [[.../customer.parquet]]}, projection=[c_custkey, c_name]
///                    RayStageExec[1] (output_partitioning=Hash([Column { name: "o_custkey", index: 0 }], 2))
///                      RepartitionExec: partitioning=Hash([o_custkey@0], 2), input_partitions=2
///                        ParquetExec: file_groups={2 groups: [[.../orders.parquet:0..19037604], [.../orders.parquet:19037604..38075207]]}, projection=[o_custkey, o_totalprice]
/// ```
/// This physical plan will be split into 4 stages, as indicated by the RayStageExec nodes.  Those
/// stages will look like this:
///
/// ```
/// Stage 0 output partitions:2 shadow partitions: 1
/// MaxRowsExec[max_rows=8192]
///   CoalesceBatchesExec: target_batch_size=8192
///     RepartitionExec: partitioning=Hash([c_custkey@0], 2), input_partitions=1
///       PartitionIsolatorExec
///         ParquetExec: file_groups={1 group: [[.../customer.parquet]]}, projection=[c_custkey, c_name]
///
/// Stage 1 output partitions:2 shadow partitions: 2
/// MaxRowsExec[max_rows=8192]
///   CoalesceBatchesExec: target_batch_size=8192
///     RepartitionExec: partitioning=Hash([o_custkey@0], 2), input_partitions=1
///       PartitionIsolatorExec
///         ParquetExec: file_groups={2 groups: [[.../orders.parquet:0..19037604], [.../orders.parquet:19037604..38075207]]}, projection=[o_custkey, o_totalprice]
///
/// Stage 2 output partitions:2 shadow partitions: 2
/// MaxRowsExec[max_rows=8192]
///   CoalesceBatchesExec: target_batch_size=8192
///     RepartitionExec: partitioning=Hash([c_name@0], 2), input_partitions=1
///       PartitionIsolatorExec
///         AggregateExec: mode=Partial, gby=[c_name@1 as c_name], aggr=[sum(o.o_totalprice)]
///           ProjectionExec: expr=[o_totalprice@1 as o_totalprice, c_name@0 as c_name]
///             HashJoinExec: mode=Partitioned, join_type=Inner, on=[(c_custkey@0, o_custkey@0)], projection=[c_name@1, o_totalprice@3]
///               RayStageReaderExec[0] (output_partitioning=UnknownPartitioning(2))
///               RayStageReaderExec[1] (output_partitioning=UnknownPartitioning(2))
///
/// Stage 3 output partitions:1 shadow partitions: None
/// MaxRowsExec[max_rows=8192]
///   CoalesceBatchesExec: target_batch_size=8192
///     ProjectionExec: expr=[c_name@0 as c_name, sum(o.o_totalprice)@1 as total]
///       GlobalLimitExec: skip=0, fetch=1
///         CoalescePartitionsExec
///           AggregateExec: mode=FinalPartitioned, gby=[c_name@0 as c_name], aggr=[sum(o.o_totalprice)]
///             RayStageReaderExec[2] (output_partitioning=UnknownPartitioning(2))
/// ```
///
/// See [`crate::isolator::PartitionIsolatorExec`] for more information on how the shadow partitions work
#[derive(Debug)]
pub struct RayStageExec {
    /// Input plan
    pub(crate) input: Arc<dyn ExecutionPlan>,
    /// Output partitioning
    properties: PlanProperties,
    pub stage_id: usize,
}

impl RayStageExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, stage_id: usize) -> Self {
        let properties = input.properties().clone();

        Self {
            input,
            properties,
            stage_id,
            // unique names
        }
    }
}
impl DisplayAs for RayStageExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "RayStageExec[{}] (output_partitioning={:?})",
            self.stage_id,
            self.properties().partitioning
        )
    }
}

impl ExecutionPlan for RayStageExec {
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn name(&self) -> &str {
        "RayStageExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        &self.properties
    }

    fn with_new_children(
        self: std::sync::Arc<Self>,
        children: Vec<std::sync::Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<std::sync::Arc<dyn ExecutionPlan>> {
        // TODO: handle more general case
        assert_eq!(children.len(), 1);
        let child = children[0].clone();
        Ok(Arc::new(RayStageExec::new(child, self.stage_id)))
    }

    /// We will have to defer this functionality to python as Ray does not yet have Rust bindings.
    fn execute(
        &self,
        _partition: usize,
        _context: std::sync::Arc<datafusion::execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        unimplemented!("Ray Stage Exec")
    }
}
