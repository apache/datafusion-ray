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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::FlightClient;
use datafusion::common::internal_datafusion_err;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_python::utils::wait_for_future;
use futures::{Stream, TryStreamExt};
use local_ip_address::local_ip;
use log::{debug, error, info, trace};
use tokio::net::TcpListener;

use tonic::transport::Server;
use tonic::{async_trait, Request, Response, Status};

use datafusion::error::Result as DFResult;

use arrow_flight::{flight_service_server::FlightServiceServer, Ticket};

use pyo3::prelude::*;

use parking_lot::{Mutex, RwLock};

use tokio::sync::mpsc::{channel, Receiver, Sender};

use crate::flight::{FlightHandler, FlightServ};
use crate::isolator::PartitionGroup;
use crate::util::{
    bytes_to_physical_plan, display_plan_with_partition_counts, extract_ticket,
    input_stage_ids, make_client, ResultExt,
};

/// a map of stage_id, partition to a list FlightClients that can serve
/// this (stage_id, and partition).   It is assumed that to consume a partition, the consumer
/// will consume the partition from all clients and merge the results.
pub(crate) struct ServiceClients(pub HashMap<(usize, usize), Mutex<Vec<FlightClient>>>);

/// StageHandler is a [`FlightHandler`] that serves streams of partitions from a hosted Physical Plan
/// It only responds to the DoGet Arrow Flight method.
struct StageHandler {
    /// our name, useful for logging
    name: String,
    /// Inner state of the handler
    inner: RwLock<Option<StageHandlerInner>>,
}

struct StageHandlerInner {
    /// the physical plan that comprises our stage
    pub(crate) plan: Arc<dyn ExecutionPlan>,
    /// the session context we will use to execute the plan
    pub(crate) ctx: SessionContext,
}

impl StageHandler {
    pub fn new(name: String) -> Self {
        let inner = RwLock::new(None);

        Self { name, inner }
    }
    async fn update_plan(
        &self,
        stage_id: usize,
        stage_addrs: HashMap<usize, HashMap<usize, Vec<String>>>,
        plan: Arc<dyn ExecutionPlan>,
        partition_group: Vec<usize>,
    ) -> DFResult<()> {
        let inner = StageHandlerInner::new(stage_id, stage_addrs, plan, partition_group).await?;
        self.inner.write().replace(inner);
        Ok(())
    }
}

impl StageHandlerInner {
    pub async fn new(
        stage_id: usize,
        stage_addrs: HashMap<usize, HashMap<usize, Vec<String>>>,
        plan: Arc<dyn ExecutionPlan>,
        partition_group: Vec<usize>,
    ) -> DFResult<Self> {
        let ctx = Self::configure_ctx(stage_id, stage_addrs, &plan, partition_group).await?;

        Ok(Self { plan, ctx })
    }

    async fn configure_ctx(
        stage_id: usize,
        stage_addrs: HashMap<usize, HashMap<usize, Vec<String>>>,
        plan: &Arc<dyn ExecutionPlan>,
        partition_group: Vec<usize>,
    ) -> DFResult<SessionContext> {
        let stage_ids_i_need = input_stage_ids(plan)?;

        // map of stage_id, partition -> Vec<FlightClient>
        let mut client_map = HashMap::new();

        // a map of address -> FlightClient which we use while building the client map above
        // so that we don't create duplicate clients for the same address.
        let mut clients = HashMap::new();

        fn clone_flight_client(c: &FlightClient) -> FlightClient {
            let inner_clone = c.inner().clone();
            FlightClient::new_from_inner(inner_clone)
        }

        for stage_id in stage_ids_i_need {
            let partition_addrs = stage_addrs.get(&stage_id).ok_or(internal_datafusion_err!(
                "Cannot find stage addr {stage_id} in {:?}",
                stage_addrs
            ))?;

            for (partition, addrs) in partition_addrs {
                let mut flight_clients = vec![];
                for addr in addrs {
                    let client = match clients.entry(addr) {
                        Entry::Occupied(o) => clone_flight_client(o.get()),
                        Entry::Vacant(v) => {
                            let client = make_client(addr).await?;
                            let clone = clone_flight_client(&client);
                            v.insert(client);
                            clone
                        }
                    };
                    flight_clients.push(client);
                }
                client_map.insert((stage_id, *partition), Mutex::new(flight_clients));
            }
        }

        let mut config = SessionConfig::new().with_extension(Arc::new(ServiceClients(client_map)));

        // this only matters if the plan includes an PartitionIsolatorExec, which looks for this
        // for this extension and will be ignored otherwise
        config = config.with_extension(Arc::new(PartitionGroup(partition_group.clone())));

        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(config)
            .build();
        let ctx = SessionContext::new_with_state(state);

        trace!("ctx configured for stage {}", stage_id);

        Ok(ctx)
    }
}

fn make_stream(
    inner: &StageHandlerInner,
    partition: usize,
) -> Result<impl Stream<Item = Result<RecordBatch, FlightError>> + Send + 'static, Status> {
    let task_ctx = inner.ctx.task_ctx();

    let stream = inner
        .plan
        .execute(partition, task_ctx)
        .inspect_err(|e| {
            error!(
                "{}",
                format!("Could not get partition stream from plan {e}")
            )
        })
        .map_err(|e| Status::internal(format!("Could not get partition stream from plan {e}")))?
        .map_err(|e| FlightError::from_external_error(Box::new(e)));

    Ok(stream)
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

        let partition = extract_ticket(ticket).map_err(|e| {
            Status::internal(format!(
                "{}, Unexpected error extracting ticket {e}",
                self.name
            ))
        })?;

        trace!(
            "{}, request for partition {} from {}",
            self.name,
            partition,
            remote_addr
        );

        let name = self.name.clone();
        let stream = self
            .inner
            .read()
            .as_ref()
            .map(|inner| make_stream(inner, partition))
            .ok_or_else(|| Status::internal(format!("{} No inner found", &name)))??;

        let out_stream = FlightDataEncoderBuilder::new()
            .build(stream)
            .map_err(move |e| {
                Status::internal(format!("{} Unexpected error building stream {e}", name))
            });

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
    pub fn new(name: String) -> PyResult<Self> {
        let name = format!("[{}]", name);
        let listener = None;
        let addr = None;

        let (all_done_tx, all_done_rx) = channel(1);
        let all_done_tx = Arc::new(Mutex::new(all_done_tx));

        let handler = Arc::new(StageHandler::new(name.clone()));

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
        self.addr.clone().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyException, _>(format!(
                "{},Couldn't get addr",
                self.name
            ))
        })
    }

    /// signal to the service that we can shutdown
    ///
    /// returns a python coroutine that should be awaited
    pub fn all_done<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        let sender = self.all_done_tx.lock().clone();

        let fut = async move {
            sender.send(()).await.to_py_err()?;
            Ok(())
        };
        pyo3_async_runtimes::tokio::future_into_py(py, fut)
    }

    /// replace the plan that this service was providing, we will do this when we want
    /// to reuse the StageService for a subsequent query
    ///
    /// returns a python coroutine that should be awaited
    pub fn update_plan<'a>(
        &self,
        py: Python<'a>,
        stage_id: usize,
        stage_addrs: HashMap<usize, HashMap<usize, Vec<String>>>,
        partition_group: Vec<usize>,
        plan_bytes: &[u8],
    ) -> PyResult<Bound<'a, PyAny>> {
        let plan = bytes_to_physical_plan(&SessionContext::new(), plan_bytes)?;

        debug!(
            "{} Received New Plan: Stage:{} my addr: {}, partition_group {:?}, stage_addrs:\n{:?}\nplan:\n{}",
            self.name,
            stage_id,
            self.addr()?,
            partition_group,
            stage_addrs,
            display_plan_with_partition_counts(&plan)
        );

        let handler = self.handler.clone();
        let name = self.name.clone();
        let fut = async move {
            handler
                .update_plan(stage_id, stage_addrs, plan, partition_group.clone())
                .await
                .to_py_err()?;
            info!(
                "{} [stage: {} pg:{:?}] updated plan",
                name, stage_id, partition_group
            );
            Ok(())
        };

        pyo3_async_runtimes::tokio::future_into_py(py, fut)
    }

    /// start the service
    /// returns a python coroutine that should be awaited
    pub fn serve<'a>(&mut self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        let mut all_done_rx = self.all_done_rx.take().unwrap();

        let signal = async move {
            all_done_rx
                .recv()
                .await
                .expect("problem receiving shutdown signal");
        };

        let service = FlightServ {
            handler: self.handler.clone(),
        };

        let svc = FlightServiceServer::new(service);

        let listener = self.listener.take().unwrap();
        let name = self.name.clone();

        let serv = async move {
            Server::builder()
                .add_service(svc)
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener),
                    signal,
                )
                .await
                .inspect_err(|e| error!("{}, ERROR serving {e}", name))
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyException, _>(format!("{e}")))?;
            Ok::<(), Box<dyn Error + Send + Sync>>(())
        };

        let fut = async move {
            serv.await.to_py_err()?;
            Ok(())
        };

        pyo3_async_runtimes::tokio::future_into_py(py, fut)
    }
}
