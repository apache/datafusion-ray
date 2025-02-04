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

use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;

use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::FlightClient;
use datafusion::common::internal_datafusion_err;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_python::utils::wait_for_future;
use futures::TryStreamExt;
use local_ip_address::local_ip;
use tokio::net::TcpListener;

use tonic::transport::Server;
use tonic::{async_trait, Request, Response, Status};

use datafusion::error::Result as DFResult;

use arrow_flight::{flight_service_server::FlightServiceServer, Ticket};

use pyo3::prelude::*;

use parking_lot::Mutex;

use tokio::sync::mpsc::{channel, Receiver, Sender};

use crate::flight::{FlightHandler, FlightServ};
use crate::isolator::ShadowPartitionNumber;
use crate::util::{
    bytes_to_physical_plan, extract_ticket, input_stage_ids, make_client, ResultExt,
};

/// a map of stage_id to a list FlightClients that can serve
/// this (stage_id, and partition).   It is assumed that to consume a partition, the consumer
/// will consume the partition from all clients and merge the results.
pub(crate) struct ServiceClients(pub HashMap<usize, Mutex<Vec<FlightClient>>>);

/// StageHandler is a [`FlightHandler`] that serves streams of partitions from a hosted Physical Plan
/// It only responds to the DoGet Arrow Flight method.
struct StageHandler {
    /// our stage id that we are hosting
    stage_id: usize,
    /// the physical plan that comprises our stage
    plan: Arc<dyn ExecutionPlan>,
    /// the session context we will use to execute the plan
    ctx: Mutex<Option<SessionContext>>,
    /// PartitionIsolator looks for this value and if present only exposes
    /// this partition of its input .
    shadow_partition_number: Option<usize>,
}

impl StageHandler {
    pub async fn new(
        stage_id: usize,
        plan_bytes: &[u8],
        shadow_partition_number: Option<usize>,
    ) -> DFResult<Self> {
        let plan = bytes_to_physical_plan(&SessionContext::new(), plan_bytes)?;

        let ctx = Mutex::new(None);

        Ok(Self {
            stage_id,
            plan,
            ctx,
            shadow_partition_number,
        })
    }

    async fn configure_ctx(&self, stage_addrs: HashMap<usize, Vec<String>>) -> DFResult<()> {
        let stage_ids_i_need = input_stage_ids(&self.plan)?;

        let mut client_map = HashMap::new();

        for stage_id in stage_ids_i_need {
            let addrs = stage_addrs
                .get(&stage_id)
                .ok_or(internal_datafusion_err!("Cannot find stage addr"))?;
            let mut clients = vec![];
            for addr in addrs {
                clients.push(make_client(addr).await?);
            }
            client_map.insert(stage_id, Mutex::new(clients));
        }

        let mut config = SessionConfig::new().with_extension(Arc::new(ServiceClients(client_map)));

        // this only matters if the plan includes an PartitionIsolatorExec, which looks for this
        // for this extension and will be ignored otherwise
        if let Some(shadow) = self.shadow_partition_number {
            config = config.with_extension(Arc::new(ShadowPartitionNumber(shadow)))
        }

        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(config)
            .build();
        let ctx = SessionContext::new_with_state(state);

        self.ctx.lock().replace(ctx);
        Ok(())
    }
}

#[async_trait]
impl FlightHandler for StageHandler {
    async fn get_stream(
        &self,
        request: Request<Ticket>,
    ) -> std::result::Result<Response<crate::flight::DoGetStream>, Status> {
        let remote_addr = request
            .remote_addr()
            .map(|a| a.to_string())
            .unwrap_or("unknown".to_string());

        let ticket = request.into_inner();

        let partition = extract_ticket(ticket)
            .map_err(|e| Status::internal(format!("Unexpected error extracting ticket {e}")))?;

        println!(
            "StageService[Stage:{}], request for partition {} from {}",
            self.stage_id, partition, remote_addr
        );

        let task_ctx = self
            .ctx
            .lock()
            .as_ref()
            .ok_or(Status::internal("get_stream cannot find ctx"))?
            .task_ctx();

        let stream = self
            .plan
            .execute(partition, task_ctx)
            .map_err(|e| Status::internal(format!("Could not get partition stream from plan {e}")))?
            .map_err(|e| FlightError::from_external_error(Box::new(e)));

        let out_stream = FlightDataEncoderBuilder::new()
            .build(stream)
            .map_err(|_| Status::internal("Unexpected error building stream {e}"));

        Ok(Response::new(Box::pin(out_stream)))
    }
}

/// StageService is a Arrow Flight service that serves streams of
/// partitions from a hosted Physical Plan
///
/// It only responds to the DoGet Arrow Flight method
#[pyclass]
pub struct StageService {
    name: String,
    listener: Option<TcpListener>,
    handler: Arc<StageHandler>,
    addr: Option<String>,
    all_done_tx: Arc<Mutex<Sender<()>>>,
    all_done_rx: Option<Receiver<()>>,
}

#[pymethods]
impl StageService {
    #[new]
    #[pyo3(signature = (stage_id, plan_bytes, shadow_partition=None))]
    pub fn new(
        py: Python,
        stage_id: usize,
        plan_bytes: &[u8],
        shadow_partition: Option<usize>,
    ) -> PyResult<Self> {
        let listener = None;
        let addr = None;

        let (all_done_tx, all_done_rx) = channel(1);
        let all_done_tx = Arc::new(Mutex::new(all_done_tx));
        let name = format!("StageService[{}]", stage_id);

        let fut = StageHandler::new(stage_id, plan_bytes, shadow_partition);

        let handler = Arc::new(wait_for_future(py, fut).to_py_err()?);

        Ok(Self {
            name,
            listener,
            handler,
            addr,
            all_done_tx,
            all_done_rx: Some(all_done_rx),
        })
    }

    /// bind the listener to a socket.  This method must complete
    /// before any other methods are called.   This is separate
    /// from new() because Ray does not let you wait (AFAICT) on Actor inits to complete
    /// and we will want to wait on this with ray.get()
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

    /// get the address of the listing socket for this service
    pub fn addr(&self) -> PyResult<String> {
        self.addr
            .clone()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyException, _>("Couldn't get addr"))
    }

    pub fn set_stage_addrs(
        &mut self,
        py: Python,
        stage_addrs: HashMap<usize, Vec<String>>,
    ) -> PyResult<()> {
        wait_for_future(py, self.handler.configure_ctx(stage_addrs)).to_py_err()
    }

    /// signal to the service that we can shutdown
    /// returns a python coroutine that should be awaited
    pub fn all_done<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        let sender = self.all_done_tx.lock().clone();

        let fut = async move {
            sender.send(()).await.to_py_err()?;
            Ok(())
        };
        pyo3_async_runtimes::tokio::future_into_py(py, fut)
    }

    /// start the service
    /// returns a python coroutine that should be awaited
    pub fn serve<'a>(&mut self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        let mut all_done_rx = self.all_done_rx.take().unwrap();

        let signal = async move {
            // TODO: handle Result
            let result = all_done_rx.recv().await;
        };

        let service = FlightServ {
            handler: self.handler.clone(),
        };

        let svc = FlightServiceServer::new(service);

        let listener = self.listener.take().unwrap();

        let name = self.name.clone();
        let serv = async move {
            println!("StageService Serving");
            Server::builder()
                .add_service(svc)
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener),
                    signal,
                )
                .await
                .inspect_err(|e| println!("StageService [{}] ERROR serving {e}", name))
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyException, _>(format!("{e}")))?;
            println!("StageService [{}] DONE serving", name);
            Ok::<(), Box<dyn Error + Send + Sync>>(())
        };

        let name = self.name.clone();
        let fut = async move {
            serv.await.to_py_err()?;
            println!("StageService [{}] both futures done. all joined", name);
            Ok(())
        };

        pyo3_async_runtimes::tokio::future_into_py(py, fut)
    }
}
