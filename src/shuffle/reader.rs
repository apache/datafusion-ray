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

use crate::shuffle::{create_object_store, CombinedRecordBatchStream};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::reader::FileReader;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Statistics;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::expressions::UnKnownColumn;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use futures::{Stream, StreamExt};
use log::debug;
use object_store::{path::Path as ObjectStorePath, ObjectStore};
use std::any::Any;
use std::fmt::Formatter;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;
use tokio::runtime::Handle;
use tokio::task::block_in_place;

#[derive(Debug)]
pub struct ShuffleReaderExec {
    /// Query stage to read from
    pub stage_id: usize,
    /// The output schema of the query stage being read from
    schema: SchemaRef,

    properties: PlanProperties,
    /// Directory to read shuffle files from
    pub shuffle_dir: String,
}

impl ShuffleReaderExec {
    pub fn new(
        stage_id: usize,
        schema: SchemaRef,
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
            EquivalenceProperties::new(schema.clone()),
            partitioning,
            datafusion::physical_plan::ExecutionMode::Unbounded,
        );

        Self {
            stage_id,
            schema,
            properties,
            shuffle_dir: shuffle_dir.to_string(),
        }
    }
}

impl ExecutionPlan for ShuffleReaderExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let mut streams: Vec<SendableRecordBatchStream> = vec![];

        for input_part in 0..self.properties.partitioning.partition_count() {
            let file = format!(
                "{}/shuffle_{}_{}_{partition}.arrow",
                self.shuffle_dir, self.stage_id, input_part
            );
            debug!(
                "ShuffleReaderExec partition {} reading from stage {} file {}",
                partition, self.stage_id, file
            );

            let object_path = ObjectStorePath::from(file.as_str());
            let object_store = create_object_store()?;

            let result: Result<Option<LocalShuffleStream>> = block_in_place(move || {
                Handle::current().block_on(async move {
                    match object_store.get(&object_path).await {
                        Ok(get_result) => {
                            println!("Downloading {file} from object storage");
                            let start = Instant::now();
                            let mut local_file = File::create(&file).map_err(|e| {
                                DataFusionError::Execution(format!(
                                    "ShuffleReaderExec failed to create file: {}",
                                    e
                                ))
                            })?;
                            let mut stream = get_result.into_stream();
                            let mut total_bytes = 0;
                            while let Some(chunk) = stream.next().await {
                                let bytes = chunk?;
                                total_bytes += bytes.len();
                                local_file.write_all(&bytes)?;
                            }
                            let end = Instant::now();
                            println!(
                                "Downloaded {file} with {total_bytes} bytes in {:?}",
                                end.duration_since(start)
                            );
                            println!("Deleting {} from object storage", object_path);
                            //object_store.delete(&object_path).await?;
                            Ok(Some(LocalShuffleStream::new(
                                PathBuf::from(&file),
                                self.schema.clone(),
                            )))
                        }
                        Err(e) => {
                            let error_message = e.to_string();
                            if error_message.contains("NotFound")
                                || error_message.contains("NoSuchKey")
                            {
                                // this is fine
                            } else {
                                println!("Download failed: {}", e);
                            }
                            Ok(None)
                        }
                    }
                })
            });

            if let Some(stream) = result? {
                streams.push(Box::pin(stream));
            }
        }
        Ok(Box::pin(CombinedRecordBatchStream::new(
            self.schema.clone(),
            streams,
        )))
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema))
    }

    fn name(&self) -> &str {
        "shuffle reader"
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }
}

impl DisplayAs for ShuffleReaderExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "ShuffleReaderExec(stage_id={}, input_partitioning={:?})",
            self.stage_id,
            self.properties().partitioning
        )
    }
}

struct LocalShuffleStream {
    file: PathBuf,
    reader: Option<FileReader<File>>,
    /// The output schema of the query stage being read from
    schema: SchemaRef,
}

impl LocalShuffleStream {
    pub fn new(file: PathBuf, schema: SchemaRef) -> Self {
        LocalShuffleStream {
            file,
            schema,
            reader: None,
        }
    }
}

impl Stream for LocalShuffleStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.reader.is_none() {
            // download the file from object storage

            let file = File::open(&self.file).map_err(|e| {
                DataFusionError::Execution(format!(
                    "ShuffleReaderExec failed to open file {}: {}",
                    self.file.display(),
                    e
                ))
            })?;
            self.reader = Some(FileReader::try_new(file, None).map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to open IPC file {}: {:?}",
                    self.file.display(),
                    e
                ))
            })?);

            // TODO reinstate
            // if self.schema != stream.schema() {
            //     return Err(DataFusionError::Internal(
            //         "Not all shuffle files have the same schema".to_string(),
            //     ));
            // }
        }
        if let Some(reader) = self.reader.as_mut() {
            if let Some(batch) = reader.next() {
                return Poll::Ready(Some(batch.map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Error reading batch from Arrow IPC file: {:?}",
                        e
                    ))
                })));
            }
            Poll::Ready(None)
        } else {
            unreachable!()
        }
    }
}

impl RecordBatchStream for LocalShuffleStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
