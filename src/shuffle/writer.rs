// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::shuffle::create_object_store;
use bytes::Bytes;
use datafusion::arrow::array::Int32Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::ipc::writer::FileWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::common::{DataFusionError, Result, Statistics};
use datafusion::execution::context::TaskContext;
use datafusion::execution::RecordBatchStream;
use datafusion::physical_expr::expressions::UnKnownColumn;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder};
use datafusion::physical_plan::repartition::BatchPartitioner;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    metrics, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion_proto::protobuf::PartitionStats;
use futures::StreamExt;
use futures::TryStreamExt;
use log::debug;
use object_store::{path::Path as ObjectStorePath, MultipartUpload, ObjectStore, PutPayload};
use std::any::Any;
use std::fmt::Formatter;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

#[derive(Debug)]
pub struct ShuffleWriterExec {
    pub stage_id: usize,
    pub(crate) plan: Arc<dyn ExecutionPlan>,
    /// Output partitioning
    properties: PlanProperties,
    /// Directory to write shuffle files from
    pub shuffle_dir: String,
    /// Metrics
    pub metrics: ExecutionPlanMetricsSet,
}

impl ShuffleWriterExec {
    pub fn new(
        stage_id: usize,
        plan: Arc<dyn ExecutionPlan>,
        partitioning: Partitioning,
        shuffle_dir: &str,
    ) -> Self {
        let partitioning = match partitioning {
            Partitioning::Hash(expr, n) if expr.is_empty() => Partitioning::UnknownPartitioning(n),
            Partitioning::Hash(expr, n) => {
                // workaround for DataFusion bug https://github.com/apache/arrow-datafusion/issues/5184
                Partitioning::Hash(
                    expr.into_iter()
                        .filter(|e| e.as_any().downcast_ref::<UnKnownColumn>().is_none())
                        .collect(),
                    n,
                )
            }
            _ => partitioning,
        };
        let properties = PlanProperties::new(
            EquivalenceProperties::new(plan.schema()),
            partitioning,
            datafusion::physical_plan::ExecutionMode::Unbounded,
        );

        Self {
            stage_id,
            plan,
            properties,
            shuffle_dir: shuffle_dir.to_string(),
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl ExecutionPlan for ShuffleWriterExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.plan.schema()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.plan]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        unimplemented!()
    }

    fn execute(
        &self,
        input_partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        debug!(
            "ShuffleWriterExec[stage={}].execute(input_partition={input_partition})",
            self.stage_id
        );

        let object_store = create_object_store()?;

        let mut stream = self.plan.execute(input_partition, context)?;

        let write_time =
            MetricBuilder::new(&self.metrics).subset_time("write_time", input_partition);
        let repart_time =
            MetricBuilder::new(&self.metrics).subset_time("repart_time", input_partition);

        let stage_id = self.stage_id;
        let partitioning = self.properties().output_partitioning().to_owned();
        let partition_count = partitioning.partition_count();
        let shuffle_dir = self.shuffle_dir.clone();

        let results = async move {
            match &partitioning {
                Partitioning::RoundRobinBatch(_) => {
                    unimplemented!()
                }
                Partitioning::UnknownPartitioning(_) => {
                    // stream the results from the query, preserving the input partitioning
                    let path =
                        format!("{shuffle_dir}/shuffle_{stage_id}_{input_partition}_0.arrow");
                    let path = Path::new(&path);
                    debug!(
                        "ShuffleWriterExec[stage={}] Writing results to {:?}",
                        stage_id, path
                    );

                    let mut rows = 0;
                    let mut writer =
                        IPCWriter::new(object_store.clone(), path, stream.schema().as_ref())?;
                    while let Some(result) = stream.next().await {
                        let input_batch = result?;
                        rows += input_batch.num_rows();
                        writer.write(&input_batch)?;
                    }
                    writer.finish().await?;

                    debug!(
                        "Query completed. Shuffle write time: {}. Rows: {}.",
                        write_time, rows
                    );
                }
                Partitioning::Hash(_, _) => {
                    // we won't necessarily produce output for every possible partition, so we
                    // create writers on demand
                    let mut writers: Vec<Option<IPCWriter>> = vec![];
                    for _ in 0..partition_count {
                        writers.push(None);
                    }

                    let mut partitioner =
                        BatchPartitioner::try_new(partitioning.clone(), repart_time.clone())?;

                    let mut rows = 0;

                    while let Some(result) = stream.next().await {
                        let input_batch = result?;
                        rows += input_batch.num_rows();

                        debug!(
                            "ShuffleWriterExec[stage={}] writing batch:\n{}",
                            stage_id,
                            pretty_format_batches(&[input_batch.clone()])?
                        );

                        partitioner.partition(input_batch, |output_partition, output_batch| {
                            match &mut writers[output_partition] {
                                Some(w) => {
                                    w.write(&output_batch)?;
                                }
                                None => {
                                    let path = format!(
                                        "{shuffle_dir}/shuffle_{stage_id}_{input_partition}_{output_partition}.arrow",
                                    );
                                    let path = Path::new(&path);
                                    debug!("ShuffleWriterExec[stage={}] Writing results to {:?}", stage_id, path);

                                    let mut writer = IPCWriter::new(object_store.clone(), path, stream.schema().as_ref())?;

                                    writer.write(&output_batch)?;
                                    writers[output_partition] = Some(writer);
                                }
                            }
                            Ok(())
                        })?;
                    }

                    for (i, w) in writers.iter_mut().enumerate() {
                        match w {
                            Some(w) => {
                                w.finish().await?;
                                debug!(
                                        "ShuffleWriterExec[stage={}] Finished writing shuffle partition {} at {:?}. Batches: {}. Rows: {}. Bytes: {}.",
                                        stage_id,
                                        i,
                                        w.path(),
                                        w.num_batches,
                                        w.num_rows,
                                        w.num_bytes
                                    );
                            }
                            None => {}
                        }
                    }
                    debug!(
                        "ShuffleWriterExec[stage={}] Finished processing stream with {rows} rows",
                        stage_id
                    );
                }
            }

            // create a dummy batch to return - later this could be metadata about the
            // shuffle partitions that were written out
            let schema = Arc::new(Schema::new(vec![
                Field::new("shuffle_repart_time", DataType::Int32, true),
                Field::new("shuffle_write_time", DataType::Int32, true),
            ]));
            let arr_repart_time = Int32Array::from(vec![repart_time.value() as i32]);
            let arr_write_time = Int32Array::from(vec![write_time.value() as i32]);
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(arr_repart_time), Arc::new(arr_write_time)],
            )?;

            // return as a stream
            MemoryStream::try_new(vec![batch], schema, None)
        };
        let schema = self.schema();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(results).try_flatten(),
        )))
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema()))
    }

    fn name(&self) -> &str {
        "shuffle writer"
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        &self.properties
    }
}

impl DisplayAs for ShuffleWriterExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "ShuffleWriterExec(stage_id={}, output_partitioning={:?})",
            self.stage_id,
            self.properties().partitioning
        )
    }
}

struct IPCWriter {
    /// object store
    object_store: Arc<dyn ObjectStore>,
    /// path
    pub path: PathBuf,
    /// inner writer
    pub writer: FileWriter<File>,
    /// batches written
    pub num_batches: usize,
    /// rows written
    pub num_rows: usize,
    /// bytes written
    pub num_bytes: usize,
}

impl IPCWriter {
    /// Create new writer
    pub fn new(object_store: Arc<dyn ObjectStore>, path: &Path, schema: &Schema) -> Result<Self> {
        let file = File::create(path).map_err(|e| {
            DataFusionError::Execution(format!(
                "ShuffleWriterExec failed to create partition file at {path:?}: {e:?}"
            ))
        })?;
        Ok(Self {
            object_store,
            num_batches: 0,
            num_rows: 0,
            num_bytes: 0,
            path: path.into(),
            writer: FileWriter::try_new(file, schema).map_err(|e| {
                DataFusionError::Execution(format!(
                    "ShuffleWriterExec failed to create IPC file: {e:?}"
                ))
            })?,
        })
    }

    /// Write one single batch
    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.writer.write(batch).map_err(|e| {
            DataFusionError::Execution(format!(
                "ShuffleWriterExec failed to write IPC batch: {e:?}"
            ))
        })?;
        self.num_batches += 1;
        self.num_rows += batch.num_rows();
        let num_bytes: usize = batch.get_array_memory_size();
        self.num_bytes += num_bytes;
        Ok(())
    }

    /// Finish the writer
    pub async fn finish(&mut self) -> Result<()> {
        self.writer.finish().map_err(|e| {
            DataFusionError::Execution(format!(
                "ShuffleWriterExec failed to finish writing IPC file: {e:?}"
            ))
        })?;

        if self.num_batches > 0 {
            // upload to object storage
            let mut file = File::open(&self.path).map_err(|e| {
                DataFusionError::Execution(format!(
                    "ShuffleWriterExec failed to open file {}: {}",
                    self.path.display(),
                    e
                ))
            })?;

            let object_store_path = ObjectStorePath::from_filesystem_path(&self.path)?;

            const MULTIPART_CHUNK_SIZE: usize = 5 * 1024 * 1024;

            let start = Instant::now();
            if self.num_bytes > MULTIPART_CHUNK_SIZE {
                // use multipart put for larger files
                println!(
                    "Uploading shuffle file {} containing {} bytes (put_multipart)",
                    &self.path.display(),
                    self.num_bytes
                );
                let mut buffer = vec![0; MULTIPART_CHUNK_SIZE];
                let mut writer = self.object_store.put_multipart(&object_store_path).await?;
                let mut total_bytes = 0;
                loop {
                    let bytes_read = read_full_buffer(&mut file, &mut buffer)?;
                    total_bytes += bytes_read;
                    if bytes_read == 0 {
                        break;
                    }
                    let bytes = Bytes::copy_from_slice(&buffer[..bytes_read]);
                    writer.put_part(PutPayload::from(bytes)).await?;
                }
                assert!(total_bytes > 0);
                let _put_result = writer.complete().await?;
            } else {
                println!(
                    "Uploading shuffle file {} containing {} bytes (put)",
                    &self.path.display(),
                    self.num_bytes
                );

                let mut buffer = Vec::with_capacity(self.num_bytes);
                let bytes_read = file.read_to_end(&mut buffer)?;
                assert!(bytes_read > 0);
                let bytes = Bytes::copy_from_slice(&buffer[..bytes_read]);
                self.object_store
                    .put(&object_store_path, PutPayload::from(bytes))
                    .await?;
            }
            let end = Instant::now();
            println!("File upload took {:?}", end.duration_since(start));
        }

        std::fs::remove_file(&self.path).map_err(|e| {
            println!("ShuffleWriterExec failed to delete file: {}", e);
            e
        })?;
        Ok(())
    }

    /// Path write to
    pub fn path(&self) -> &Path {
        &self.path
    }
}

fn read_full_buffer(file: &mut File, buffer: &mut [u8]) -> Result<usize> {
    let mut total_read = 0;
    while total_read < buffer.len() {
        match file.read(&mut buffer[total_read..])? {
            0 => break,
            bytes_read => total_read += bytes_read,
        }
    }
    Ok(total_read)
}
