use crate::isolator::{PartitionIsolatorExec, ShadowPartitionNumber};
use crate::protobuf::StreamMeta;
use crate::ray_stage_reader::RayStageReaderExec;
use crate::util::{bytes_to_physical_plan, make_client, physical_plan_to_bytes, ResultExt};
use arrow::datatypes::Schema;
use arrow::pyarrow::PyArrowType;
use arrow::util::pretty::pretty_format_batches;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_descriptor::DescriptorType;
use arrow_flight::{FlightClient, FlightDescriptor};
use async_stream::stream;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{internal_datafusion_err, internal_err};
use datafusion::physical_plan::{collect, displayable};
use datafusion_python::physical_plan::PyExecutionPlan;
use futures::future::{join_all, try_join_all};
use futures::stream::FuturesUnordered;
use object_store::aws::AmazonS3Builder;
use prost::Message;
use rust_decimal::prelude::*;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use url::Url;

use datafusion::execution::{SessionStateBuilder, TaskContext};
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_python::utils::wait_for_future;
use futures::{StreamExt, TryStreamExt};
use pyo3::prelude::*;

use anyhow::Result;

/// a map of (stage_id, partition_id) to FlightClient used to speak to that Exchanger
pub(crate) struct ExchangeAddrs(pub HashMap<(usize, usize), String>);

#[pyclass]
pub struct PyStage {
    name: String,
    #[pyo3(get)]
    stage_id: usize,
    pub(crate) plan: Arc<dyn ExecutionPlan>,
    ctx: SessionContext,
    fraction: f64,
    shadow_partition_number: Option<usize>,
    required_output_partitions: Vec<usize>,
}

#[pymethods]
impl PyStage {
    #[new]
    #[pyo3(signature = (stage_id, plan_bytes, exchange_addrs, required_output_partitions, shadow_partition_number=None, bucket=None, fraction=1.0))]
    pub fn from_bytes(
        stage_id: usize,
        plan_bytes: Vec<u8>,
        exchange_addrs: HashMap<(usize, usize), String>,
        required_output_partitions: Vec<usize>,
        shadow_partition_number: Option<usize>,
        bucket: Option<String>,
        fraction: f64,
    ) -> PyResult<Self> {
        let name = format!(
            "PyStage[{}-{}]",
            stage_id,
            shadow_partition_number
                .map(|s| s.to_string())
                .unwrap_or("n/a".into())
        );

        let mut config =
            SessionConfig::new().with_extension(Arc::new(ExchangeAddrs(exchange_addrs)));

        // this only matters if the plan includes an PartitionIsolatorExec
        // and will be ignored otherwise
        if let Some(shadow) = shadow_partition_number {
            config = config.with_extension(Arc::new(ShadowPartitionNumber(shadow)))
        }

        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(config)
            .build();
        let ctx = SessionContext::new_with_state(state);

        if let Some(bucket) = bucket {
            let s3 = AmazonS3Builder::from_env().with_bucket_name(&bucket);

            let s3 = s3.build().to_py_err()?;

            let path = format!("s3://{bucket}");
            let s3_url = Url::parse(&path).to_py_err()?;
            let arc_s3 = Arc::new(s3);
            ctx.register_object_store(&s3_url, arc_s3.clone());
            println!("registered object store {s3_url}");
        }

        println!("{name} creating physical plan from bytes");
        let plan = bytes_to_physical_plan(&ctx, &plan_bytes).to_py_err()?;
        println!(
            "{name} created physical plan:\n{}",
            displayable(plan.as_ref()).indent(true)
        );

        Ok(Self {
            name,
            stage_id,
            plan,
            ctx,
            fraction,
            shadow_partition_number,
            required_output_partitions,
        })
    }

    pub fn execute(&mut self, py: Python) -> PyResult<()> {
        println!("{} executing", self.name);

        let addrs = &self
            .ctx
            .state()
            .config()
            .get_extension::<ExchangeAddrs>()
            .ok_or(internal_datafusion_err!("Flight Client not in context"))?
            .clone()
            .0;

        let futs = self.required_output_partitions.iter().map(|partition| {
            let ctx = self.ctx.task_ctx();
            let plan = self.plan.clone();
            let stage_id = self.stage_id.clone();
            let fraction = self.fraction.clone();
            let shadow_partition_number = self.shadow_partition_number.clone();

            // TODO propagate these errors appropriately
            async move {
                let client = addrs
                    .get(&(stage_id, *partition))
                    .map(|addr| make_client(addr))
                    .expect("cannot find addr")
                    .await
                    .expect("cannot make client");

                tokio::spawn(consume_stage(
                    stage_id,
                    shadow_partition_number,
                    fraction,
                    ctx,
                    *partition,
                    plan,
                    client,
                ));
            }
        });

        wait_for_future(py, join_all(futs));

        Ok(())
    }

    pub fn num_output_partitions(&self) -> usize {
        self.plan.output_partitioning().partition_count()
    }

    pub fn execution_plan(&self) -> PyExecutionPlan {
        PyExecutionPlan::new(self.plan.clone())
    }

    pub fn schema(&self) -> PyArrowType<Schema> {
        let schema = (*self.plan.schema()).clone();
        PyArrowType(schema)
    }

    pub fn plan_bytes(&self) -> PyResult<Cow<[u8]>> {
        let plan_bytes = physical_plan_to_bytes(self.plan.clone())?;
        Ok(Cow::Owned(plan_bytes))
    }
}

pub async fn consume_stage(
    stage_id: usize,
    shadow_partition_number: Option<usize>,
    fraction: f64,
    ctx: Arc<TaskContext>,
    partition: usize,
    plan: Arc<dyn ExecutionPlan>,
    mut client: FlightClient,
) -> Result<()> {
    let name = format!(
        "PyStage[{}:output partition:{}-s{}]",
        stage_id,
        partition,
        shadow_partition_number
            .map(|s| s.to_string())
            .unwrap_or("n/a".into())
    );

    println!("{name} consuming");
    let fraction = Decimal::from_f64(fraction)
        .ok_or(internal_datafusion_err!(
            "{name}: error converting fraction to decimal"
        ))?
        .to_string();

    let stream_meta = StreamMeta {
        stage_id: stage_id as u64,
        partition: partition as u64,
        fraction,
    };

    let descriptor = FlightDescriptor {
        r#type: DescriptorType::Cmd.into(),
        cmd: stream_meta.encode_to_vec().into(),
        path: vec![],
    };
    let mut total_rows = 0;
    let mut plan_output_stream = plan.execute(partition, ctx)?;

    let name_c = name.clone();
    let counting_stream = stream! {
        println!("{name_c}: starting plan consumption");
        let mut got_first_batch = false;
        while let Some(batch) = plan_output_stream.next().await {
            if !got_first_batch {
                println!("{name_c}: got first batch");
                got_first_batch = true;
            }
            total_rows += batch.as_ref().map(|b| b.num_rows()).unwrap_or(0);

            yield batch;
        }
        println!(
            "{name_c} produced {total_rows} total rows",
        );
    };

    let flight_data_stream = FlightDataEncoderBuilder::new()
        .with_flight_descriptor(Some(descriptor))
        .with_schema(plan.schema())
        .build(counting_stream.map_err(|e| FlightError::from_external_error(Box::new(e))));

    let name_c = name.clone();
    let mut response = client
        .do_put(flight_data_stream)
        .await
        .map_err(|e| internal_datafusion_err!("{name_c}: error getting back do put result: {e}"))?;

    let name_c = name.clone();
    while let Some(result) = response.next().await {
        if let Err(e) = result {
            return Err(internal_datafusion_err!(
                "{name_c}: error getting back do put result: {e}"
            )
            .into());
        }
    }
    Ok(())
}
