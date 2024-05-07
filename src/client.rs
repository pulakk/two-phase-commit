//!
//! client.rs
//! Implementation of 2PC client
//!
extern crate ipc_channel;
extern crate log;
extern crate stderrlog;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use client::ipc_channel::ipc::IpcReceiver as Receiver;
use client::ipc_channel::ipc::IpcSender as Sender;

use message;

use crate::message::ProtocolMessage;

// Client state and primitives for communicating with the coordinator
#[derive(Debug)]
pub struct Client {
    pub id_str: String,
    pub running: Arc<AtomicBool>,
    pub num_requests: u32,
    pub tx: Sender<ProtocolMessage>,
    pub rx: Receiver<ProtocolMessage>,
    pub successful_ops: u32,
    pub failed_ops: u32,
}

///
/// Client Implementation
/// Required:
/// 1. new -- constructor
/// 2. pub fn report_status -- Reports number of committed/aborted/unknown
/// 3. pub fn protocol(&mut self, n_requests: i32) -- Implements client side protocol
///
impl Client {

    ///
    /// new()
    ///
    /// Constructs and returns a new client, ready to run the 2PC protocol
    /// with the coordinator.
    ///
    /// HINT: You may want to pass some channels or other communication
    ///       objects that enable coordinator->client and client->coordinator
    ///       messaging to this constructor.
    /// HINT: You may want to pass some global flags that indicate whether
    ///       the protocol is still running to this constructor
    ///
    pub fn new(id_str: String,
               running: Arc<AtomicBool>,
               tx: Sender<ProtocolMessage>,
               rx: Receiver<ProtocolMessage>) -> Client {
        Client {
            id_str: id_str,
            running: running,
            num_requests: 0,
            tx,
            rx,
            successful_ops: 0,
            failed_ops: 0,
        }
    }

    ///
    /// wait_for_exit_signal(&mut self)
    /// Wait until the running flag is set by the CTRL-C handler
    ///
    pub fn wait_for_exit_signal(&mut self) {
        trace!("{}::Waiting for exit signal", self.id_str.clone());

        while self.running.load(Ordering::SeqCst) {};

        trace!("{}::Exiting", self.id_str.clone());
    }

    ///
    /// send_next_operation(&mut self)
    /// Send the next operation to the coordinator
    ///
    pub fn send_next_operation(&mut self) {

        // Create a new request with a unique TXID.
        self.num_requests = self.num_requests + 1;
        let txid = format!("{}_op_{}", self.id_str.clone(), self.num_requests);
        let pm = message::ProtocolMessage::generate(
            message::MessageType::ClientRequest,
            txid.clone(),
            self.id_str.clone(),
            self.num_requests
        );
        info!("{}::Sending operation #{}", self.id_str.clone(), self.num_requests);

        self.tx.send(pm).unwrap();

        trace!("{}::Sent operation #{}", self.id_str.clone(), self.num_requests);
    }

    ///
    /// recv_result()
    /// Wait for the coordinator to respond with the result for the
    /// last issued request. Note that we assume the coordinator does
    /// not fail in this simulation
    ///
    pub fn recv_result(&mut self) {
        info!("{}::Receiving Coordinator Result", self.id_str.clone());
        let result = self.rx.recv().unwrap();
    
        match result.mtype {
            message::MessageType::ClientResultCommit => self.successful_ops += 1,
            message::MessageType::ClientResultAbort => self.failed_ops += 1,
            _ => {},
        }
        info!("Client {} received response: {:?}", self.id_str, result);
    }

    ///
    /// report_status()
    /// Report the abort/commit/unknown status (aggregate) of all transaction
    /// requests made by this client before exiting.
    ///
    pub fn report_status(&mut self) {
        // TODO: Collect actual stats
        let successful_ops = self.successful_ops;
        let failed_ops = self.failed_ops;
        let unknown_ops = self.num_requests - successful_ops - failed_ops;

        println!("{:16}:\tCommitted: {:6}\tAborted: {:6}\tUnknown: {:6}", self.id_str.clone(), successful_ops, failed_ops, unknown_ops);
    }

    ///
    /// protocol()
    /// Implements the client side of the 2PC protocol
    /// HINT: if the simulation ends early, don't keep issuing requests!
    /// HINT: if you've issued all your requests, wait for some kind of
    ///       exit signal before returning from the protocol method!
    ///
    pub fn protocol(&mut self, n_requests: u32) {
        for _ in 0..n_requests {
            self.send_next_operation();
            self.recv_result();
        }

        info!("Client {} done.", self.id_str);

        // self.wait_for_exit_signal();
        self.report_status();
    }
}
