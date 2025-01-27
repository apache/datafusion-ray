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
use arrow_flight::error::FlightError;
use arrow_flight::utils::flight_data_to_arrow_batch;
use async_stream::stream;
use datafusion::common::internal_datafusion_err;
use datafusion_python::utils::wait_for_future;
use futures::future::try_join;
use futures::TryStreamExt;
use local_ip_address::local_ip;
use rust_decimal::prelude::*;
use tokio::net::TcpListener;
use tokio_stream::StreamExt;

use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

use datafusion::error::Result as DFResult;

use arrow_flight::{flight_service_server::FlightServiceServer, FlightData, PutResult, Ticket};

use pyo3::prelude::*;

use parking_lot::Mutex;

use tokio::sync::mpsc::{channel, Receiver, Sender};

use crate::flight::{FlightHandler, FlightServ};
use crate::util::{extract_stream_meta, extract_ticket, flight_data_to_schema, ResultExt};

#[derive(Hash, PartialEq, Eq, Copy, Clone)]
struct PartitionKey {
    stage_num: usize,
    partition_num: usize,
}

#[derive(Debug)]
pub struct Stats {
    pub stage_num: usize,
    pub partition_num: usize,
    pub in_out: String,
    pub total_rows: usize,
    pub remote_addr: String,
}

type StatMap = HashMap<usize, HashMap<String, HashMap<usize, HashMap<String, usize>>>>;

fn format_stats(sm: &StatMap) -> String {
    let mut out = String::new();

    for (stage_num, in_out_map) in sm {
        out.push_str(&format!("Stage: {stage_num}:\n"));
        for (in_out, part_map) in in_out_map {
            let mut total = 0;
            out.push_str(&format!("  {in_out}:\n"));
            for (part_num, addr_map) in part_map {
                out.push_str(&format!("    Partition: {part_num}:\n"));
                for (addr, rows) in addr_map {
                    total += rows;
                    out.push_str(&format!("      {addr}: {rows}\n"));
                }
            }
            out.push_str(&format!("    total rows: {total}\n"));
        }
    }

    out
}

#[derive(Debug)]
pub struct ExchangeStats {
    stats: StatMap,
    stats_receiver: Receiver<Stats>,
}

impl ExchangeStats {
    pub fn new(stats_receiver: Receiver<Stats>) -> Self {
        Self {
            stats: HashMap::new(),
            stats_receiver,
        }
    }
    async fn consume_stats(mut self) -> StatMap {
        while let Some(stat) = self.stats_receiver.recv().await {
            println!("got stat: {stat:?}");

            self.stats
                .entry(stat.stage_num)
                .or_default()
                .entry(stat.in_out)
                .or_default()
                .entry(stat.partition_num)
                .or_default()
                .insert(stat.remote_addr, stat.total_rows);
        }
        self.stats
    }
}

pub struct Exchange<T> {
    senders: Arc<Mutex<HashMap<PartitionKey, Sender<T>>>>,
    receivers: Arc<Mutex<HashMap<PartitionKey, Receiver<T>>>>,
    created: Arc<Mutex<HashSet<PartitionKey>>>,
    dones: Arc<Mutex<HashMap<PartitionKey, Decimal>>>,
    stats_sender: Arc<Mutex<Option<Sender<Stats>>>>,
}

impl<T> Exchange<T> {
    pub fn new(stats_sender: Sender<Stats>) -> Self {
        Self {
            senders: Arc::new(Mutex::new(HashMap::new())),
            receivers: Arc::new(Mutex::new(HashMap::new())),
            created: Arc::new(Mutex::new(HashSet::new())),
            dones: Arc::new(Mutex::new(HashMap::new())),
            stats_sender: Arc::new(Mutex::new(Some(stats_sender))),
        }
    }

    fn create_if_required(&self, key: PartitionKey) {
        let mut created = self.created.lock();

        if !created.contains(&key) {
            let mut senders = self.senders.lock();
            let mut receivers = self.receivers.lock();
            let mut dones = self.dones.lock();

            let (sender, receiver) = channel(100); // TODO: what size?
            senders.insert(key, sender);
            receivers.insert(key, receiver);
            dones.insert(key, Decimal::zero());
            created.insert(key);
        }
    }

    fn shutdown(&self) {
        println!("shutdown stats sending channel");
        self.stats_sender.lock().take();
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
        let remote_addr = request
            .remote_addr()
            .map(|a| a.to_string())
            .unwrap_or("unknown".to_string());

        let ticket = request.into_inner();

        let (stage_num, partition_num, _) = extract_ticket(ticket)
            .map_err(|e| Status::internal(format!("Unexpected error extracting ticket {e}")))?;

        let name = format!("[Exchange::get_stream {}:{}]", stage_num, partition_num);

        let mut recv = self
            .get(stage_num, partition_num)
            .map_err(|e| Status::internal(format!("Unexpected error getting recv channel {e}")))?;

        let mut total_rows = 0;
        let stats_sender = self
            .stats_sender
            .lock()
            .clone()
            .ok_or(Status::internal("expected stats_sender"))?;

        let stream = stream! {
            while let Some(batch) = recv.recv().await {

                total_rows += batch.num_rows();
                //println!( "{}: sending {} rows", name, batch.num_rows());
                yield Ok(batch);

            }
            if let Err(e) = stats_sender.send(Stats{
                stage_num,
                partition_num,
                in_out: "out".to_string(),
                total_rows,
                remote_addr,
            }).await {
                yield Err(FlightError::from_external_error(Box::new(
                    internal_datafusion_err!("error sending stats: {e}"))));
            }
            println!("{}: snt {} total rows", name, total_rows);

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
        let remote_addr = request
            .remote_addr()
            .map(|a| a.to_string())
            .unwrap_or("unknown".to_string());

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

        {
            // block to calculate if we are done with this partition under mutex lock
            let mut done_guard = self.dones.lock();

            let mut done = done_guard
                .remove(&key)
                .ok_or(Status::internal("expected to find done fraction"))?;

            done += done_fraction;

            let done_str = format!("{:.5}", done.round_dp(6));
            println!(
                "{}: received {} total_rows, done = {}, done_fraction {}",
                name, total_rows, done_str, done_fraction,
            );

            // crude rounding
            if done_str == "1.00000" {
                // we are done!
                println!("{}: all done", name);
                // remove the sender so all senders can be dropped and it will close the channel
                self.senders.lock().remove(&key);
            } else {
                // still more partitions to report in
                done_guard.insert(key, done);
            }
        }

        let stats_sender = self
            .stats_sender
            .lock()
            .clone()
            .ok_or(Status::internal("expected stats_sender"))?;

        stats_sender
            .send(Stats {
                stage_num,
                partition_num,
                in_out: "in".to_string(),
                total_rows,
                remote_addr,
            })
            .await
            .map_err(|e| Status::internal(format!("{name} Unexpected error sending stats {e}")))?;

        let out_stream = stream! {
            yield Ok(PutResult{app_metadata: vec![].into()});
        };

        Ok(Response::new(Box::pin(out_stream)))
    }
}

#[pyclass]
pub struct PyExchange {
    name: String,
    listener: Option<TcpListener>,
    addr: Option<String>,
    all_done_tx: Arc<Mutex<Sender<()>>>,
    all_done_rx: Option<Receiver<()>>,
}

#[pymethods]
impl PyExchange {
    #[new]
    pub fn new(name: String) -> PyResult<Self> {
        let listener = None;
        let addr = None;

        let (all_done_tx, all_done_rx) = channel(1);
        let all_done_tx = Arc::new(Mutex::new(all_done_tx));

        Ok(Self {
            name,
            listener,
            addr,
            all_done_tx,
            all_done_rx: Some(all_done_rx),
        })
    }

    /// bind the listener to a socket.  This method must complete
    /// before any other methods are called.   This is separate
    /// from new() because Ray does not let you wait (AFAICT) on Actor inits to complete
    pub fn start_up(&mut self, py: Python) -> PyResult<()> {
        let my_local_ip = local_ip().to_py_err()?;
        let my_host_str = format!("{my_local_ip}:0");
        self.listener = Some(wait_for_future(py, TcpListener::bind(&my_host_str)).to_py_err()?);

        self.addr = Some(format!(
            "{}",
            self.listener.as_ref().unwrap().local_addr().unwrap()
        ));

        Ok(())
    }

    pub fn addr(&self) -> PyResult<String> {
        self.addr
            .clone()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyException, _>("Couldn't get addr"))
    }

    pub fn all_done<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        let sender = self.all_done_tx.lock().clone();

        let fut = async move {
            sender.send(()).await.to_py_err()?;
            Ok(())
        };
        pyo3_async_runtimes::tokio::future_into_py(py, fut)
    }

    fn serve<'a>(&mut self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        // TODO: what channel size?
        let (stats_sender, stats_receiver) = channel(100);
        let exchange_stats = ExchangeStats::new(stats_receiver);

        let exchange = Arc::new(Exchange::new(stats_sender));

        let mut all_done_rx = self.all_done_rx.take().unwrap();

        let name = self.name.clone();
        let signal = async move {
            // TODO: handle Result
            println!("Exchange[{}] awaiting the done signal", name);
            let result = all_done_rx.recv().await;
            println!("Exchange[{}] got done signal {:?}", name, result);
        };

        let name = self.name.clone();
        let consume_fut = async move {
            println!("Exchange[{}] consuming stats", name);
            let stats = exchange_stats.consume_stats().await;
            println!("Exchange[{}] got stats:\n{}", name, format_stats(&stats));
            Ok::<(), PyErr>(())
        };

        let service = FlightServ {
            handler: exchange.clone(),
        };

        let svc = FlightServiceServer::new(service);

        let listener = self.listener.take().unwrap();

        let name = self.name.clone();
        let serv = async move {
            println!("Exchange[{}] Serving", name);
            Server::builder()
                .add_service(svc)
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener),
                    signal,
                )
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyException, _>(format!("{e}")))?;
            exchange.shutdown();
            println!("Exchange[{}] DONE serving", name);
            Ok(())
        };

        let name = self.name.clone();
        let fut = async move {
            try_join(consume_fut, serv).await?;
            println!("Exchange[{}] both futures done. all joined", name);
            Ok(())
        };

        pyo3_async_runtimes::tokio::future_into_py(py, fut)
    }
}

#[cfg(test)]
mod test {

    use super::*;

    #[tokio::test]
    async fn test_exchange() {
        let (stats_sender, _) = channel(100);
        let e = Exchange::<u32>::new(stats_sender);
        let msg = 1u32;

        let mut receiver = e.get(0, 0).unwrap();
        let sender = e.put(0, 0).unwrap();

        let result = sender.send(msg).await;
        assert!(result.is_ok());

        let out = receiver.recv().await.unwrap();
        assert_eq!(msg, out);
    }
}
