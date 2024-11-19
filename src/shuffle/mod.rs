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

use arrow::record_batch::RecordBatch;
use datafusion::arrow;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use futures::Stream;
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::macros::support::thread_rng_n;

mod codec;
mod reader;
mod writer;

pub use codec::ShuffleCodec;
pub use reader::ShuffleReaderExec;
pub use writer::ShuffleWriterExec;

/// CombinedRecordBatchStream can be used to combine a Vec of SendableRecordBatchStreams into one
pub struct CombinedRecordBatchStream {
    /// Schema wrapped by Arc
    schema: SchemaRef,
    /// Stream entries
    entries: Vec<SendableRecordBatchStream>,
}

impl CombinedRecordBatchStream {
    /// Create an CombinedRecordBatchStream
    pub fn new(schema: SchemaRef, entries: Vec<SendableRecordBatchStream>) -> Self {
        Self { schema, entries }
    }
}

impl RecordBatchStream for CombinedRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for CombinedRecordBatchStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use Poll::*;

        let start = thread_rng_n(self.entries.len() as u32) as usize;
        let mut idx = start;

        for _ in 0..self.entries.len() {
            let stream = self.entries.get_mut(idx).unwrap();

            match Pin::new(stream).poll_next(cx) {
                Ready(Some(val)) => return Ready(Some(val)),
                Ready(None) => {
                    // Remove the entry
                    self.entries.swap_remove(idx);

                    // Check if this was the last entry, if so the cursor needs
                    // to wrap
                    if idx == self.entries.len() {
                        idx = 0;
                    } else if idx < start && start <= self.entries.len() {
                        // The stream being swapped into the current index has
                        // already been polled, so skip it.
                        idx = idx.wrapping_add(1) % self.entries.len();
                    }
                }
                Pending => {
                    idx = idx.wrapping_add(1) % self.entries.len();
                }
            }
        }

        // If the map is empty, then the stream is complete.
        if self.entries.is_empty() {
            Ready(None)
        } else {
            Pending
        }
    }
}

pub(crate) fn create_object_store() -> Result<Arc<dyn ObjectStore>> {
    let access_key_id = std::env::var("AWS_ACCESS_KEY_ID").unwrap_or_default();
    let secret_access_key = std::env::var("AWS_SECRET_ACCESS_KEY").unwrap_or_default();
    if access_key_id.is_empty() || secret_access_key.is_empty() {
        println!("Warning! AWS_ACCESS_KEY_ID and/or AWS_SECRET_ACCESS_KEY are not defined");
    }

    // TODO configs
    let bucket_name = "dfray";
    let region = "us-east-1";
    let endpoint = "http://127.0.0.1:9000";

    Ok(Arc::new(
        AmazonS3Builder::new()
            .with_endpoint(endpoint)
            .with_allow_http(true)
            .with_region(region)
            .with_bucket_name(bucket_name)
            .with_access_key_id(access_key_id)
            .with_secret_access_key(secret_access_key)
            .build()?,
    ))
}
