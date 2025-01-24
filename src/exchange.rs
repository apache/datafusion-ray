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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::Result;

use arrow::array::RecordBatch;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::utils::flight_data_to_arrow_batch;
use async_stream::stream;
use datafusion::common::internal_datafusion_err;
use datafusion_python::utils::wait_for_future;
use futures::TryStreamExt;
use local_ip_address::local_ip;
use tokio::net::TcpListener;
use tokio_stream::StreamExt;

use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

use datafusion::error::Result as DFResult;

use arrow_flight::{flight_service_server::FlightServiceServer, FlightData, PutResult, Ticket};

use pyo3::prelude::*;

use parking_lot::Mutex;

//use async_channel::{bounded, Receiver, Sender};
use tokio::sync::mpsc::{channel, Receiver, Sender};

use crate::flight::{FlightHandler, FlightServ};
use crate::util::{extract_stream_meta, extract_ticket, flight_data_to_schema, ResultExt};

#[derive(Hash, PartialEq, Eq, Copy, Clone)]
struct PartitionKey {
    stage_num: usize,
    partition_num: usize,
}

pub struct Exchange<T> {
    senders: Arc<Mutex<HashMap<PartitionKey, Sender<T>>>>,
    receivers: Arc<Mutex<HashMap<PartitionKey, Receiver<T>>>>,
    created: Arc<Mutex<HashSet<PartitionKey>>>,
    dones: Arc<Mutex<HashMap<PartitionKey, f64>>>,
}

impl<T> Default for Exchange<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Exchange<T> {
    pub fn new() -> Self {
        Self {
            senders: Arc::new(Mutex::new(HashMap::new())),
            receivers: Arc::new(Mutex::new(HashMap::new())),
            created: Arc::new(Mutex::new(HashSet::new())),
            dones: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn create_if_required(&self, key: PartitionKey) {
        let mut created = self.created.lock();

        if !created.contains(&key) {
            let mut senders = self.senders.lock();
            let mut receivers = self.receivers.lock();
            let mut dones = self.dones.lock();

            //let (sender, receiver) = bounded(10); // TODO: what size?
            let (sender, receiver) = channel(10); // TODO: what size?
            senders.insert(key, sender);
            receivers.insert(key, receiver);
            dones.insert(key, 0.0);
            created.insert(key);
        }
    }

    pub fn put(&self, stage_num: usize, partition_num: usize) -> DFResult<Sender<T>> {
        let key = PartitionKey {
            stage_num,
            partition_num,
        };

        self.create_if_required(key);

        if let Some(sender) = self.senders.lock().get(&key) {
            Ok(sender.clone())
        } else {
            Err(internal_datafusion_err!("channel not found"))
        }
    }

    pub fn get(&self, stage_num: usize, partition_num: usize) -> DFResult<Receiver<T>> {
        let key = PartitionKey {
            stage_num,
            partition_num,
        };
        self.create_if_required(key);

        if let Some(recv) = self.receivers.lock().remove(&key) {
            Ok(recv)
        } else {
            Err(internal_datafusion_err!("channel not found"))
        }
    }
}

#[tonic::async_trait]
impl FlightHandler for Exchange<RecordBatch> {
    async fn get_stream(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<crate::flight::DoGetStream>, Status> {
        let ticket = request.into_inner();

        let (stage_num, partition_num, _) = extract_ticket(ticket)
            .map_err(|e| Status::internal(format!("Unexpected error extracting ticket {e}")))?;

        let name = format!("[Exchange::get_stream {}:{}]", stage_num, partition_num);

        let mut recv = self
            .get(stage_num, partition_num)
            .map_err(|e| Status::internal(format!("Unexpected error getting recv channel {e}")))?;

        let mut total_rows = 0;
        let stream = stream! {
            while let Some(batch) = recv.recv().await {

                total_rows += batch.num_rows();
                //println!( "{}: sending {} rows", name, batch.num_rows());
                yield Ok(batch);

            }
            println!("{}: sent {} total rows", name, total_rows);

        };
        let out_stream = FlightDataEncoderBuilder::new()
            .build(stream)
            .map_err(|_| Status::internal("Unexpected error building stream {e}"));
        Ok(Response::new(Box::pin(out_stream)))
    }

    async fn put_stream(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<crate::flight::DoPutStream>, Status> {
        let mut flight_data_stream = request.into_inner();

        // handle first message
        let ((stage_num, partition_num, done_fraction), schema) =
            match flight_data_stream.next().await {
                Some(Ok(fd)) => extract_stream_meta(&fd)
                    .and_then(|tuple| flight_data_to_schema(&fd).map(|schema| (tuple, schema)))
                    .map_err(|e| Status::internal(format!("Unexpected error extracting meta {e}"))),

                Some(Err(e)) => Err(Status::internal(format!(
                    "Unexpected error reading first batch {e}"
                ))),
                None => Err(Status::internal(
                    "Received None reading first batch".to_string(),
                )),
            }?;

        let name = format!("[Exchange::put_stream {}:{}]", stage_num, partition_num);

        let dictionaries_by_id = HashMap::new();
        let mut total_rows = 0;

        let sender = self.put(stage_num, partition_num).map_err(|e| {
            Status::internal(format!("{name} Unexpected error geting sender channel {e}"))
        })?;

        // TODO spawn a task for this?
        while let Some(flight_data) = flight_data_stream.next().await {
            let flight_data = flight_data.map_err(|e| {
                Status::internal(format!("{name} Unexpected error reading batch {e}"))
            })?;

            let batch =
                flight_data_to_arrow_batch(&flight_data, schema.clone(), &dictionaries_by_id)
                    .map_err(|e| {
                        Status::internal(format!("{name} Unexpected error making batch {e}"))
                    })?;

            total_rows += batch.num_rows();
            //println!("{}: writing {} to channel", name, batch.num_rows());
            let res = sender.send(batch).await;

            res.map_err(|e| {
                Status::internal(format!("{name} Unexpected error sending record batch {e}"))
            })?;
        }
        // ok now we're done, so mark our done fraction
        let key = PartitionKey {
            stage_num,
            partition_num,
        };
        let mut done = self
            .dones
            .lock()
            .remove(&key)
            .ok_or(Status::internal("expected to find done fraction"))?;
        done += done_fraction;

        println!(
            "{}: received {} total_rows, done = {}, done_fraction {}",
            name, total_rows, done, done_fraction,
        );

        if (done * 10000.0) as u32 == 10000u32 {
            // we are done!
            println!("{}: all done", name);
            // remove the sender so all senders can be dropped and it will close the channel
            self.senders.lock().remove(&key);
        } else {
            // still more partitions to report in
            self.dones.lock().insert(key, done);
        }

        let out_stream = stream! {
            yield Ok(PutResult{app_metadata: vec![].into()});
        };

        Ok(Response::new(Box::pin(out_stream)))
    }
}

#[pyclass]
pub struct PyExchange {
    listener: Option<TcpListener>,
}

#[pymethods]
impl PyExchange {
    #[new]
    pub fn new(py: Python) -> PyResult<Self> {
        let my_local_ip = local_ip().to_py_err()?;
        let my_host_str = format!("{my_local_ip}:0");
        let listener = Some(wait_for_future(py, TcpListener::bind(&my_host_str)).to_py_err()?);

        Ok(Self { listener })
    }

    pub fn addr(&self) -> PyResult<String> {
        self.listener
            .as_ref()
            .map(|l| l.local_addr().map(|addr| format!("{addr}")))
            .transpose()
            .to_py_err()?
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("listener not bound"))
            .to_py_err()
    }

    fn serve(&mut self, py: Python) -> PyResult<()> {
        let exchange = Exchange::new();
        let service = FlightServ {
            handler: Arc::new(exchange),
        };

        let svc = FlightServiceServer::new(service);

        let fut = Server::builder().add_service(svc).serve_with_incoming(
            tokio_stream::wrappers::TcpListenerStream::new(self.listener.take().unwrap()),
        );

        println!("PyExchange Serving");
        wait_for_future(py, fut).to_py_err()?;
        println!("PyExchange DONE serving");

        Ok(())
    }
}

#[cfg(test)]
mod test {

    use super::*;

    #[tokio::test]
    async fn test_exchange() {
        let e = Exchange::<u32>::new();
        let msg = 1u32;

        let mut receiver = e.get(0, 0).unwrap();
        let sender = e.put(0, 0).unwrap();

        let result = sender.send(msg).await;
        assert!(result.is_ok());

        let out = receiver.recv().await.unwrap();
        assert_eq!(msg, out);
    }
}
