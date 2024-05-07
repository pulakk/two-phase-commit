//!
//! participant.rs
//! Implementation of 2PC participant
//!
extern crate ipc_channel;
extern crate log;
extern crate rand;
extern crate stderrlog;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use participant::rand::prelude::*;
use participant::ipc_channel::ipc::IpcReceiver as Receiver;
use participant::ipc_channel::ipc::IpcSender as Sender;

use message::MessageType;
use message::ProtocolMessage;
use oplog;

///
/// ParticipantState
/// enum for Participant 2PC state machine
///
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParticipantState {
    Quiescent,
    ReceivedP1,
    VotedAbort,
    VotedCommit,
    AwaitingGlobalDecision,
}

///
/// Participant
/// Structure for maintaining per-participant state and communication/synchronization objects to/from coordinator
///
#[allow(dead_code)]
#[derive(Debug)]
pub struct Participant {
    id_str: String,
    state: ParticipantState,
    log: oplog::OpLog,
    running: Arc<AtomicBool>,
    send_success_prob: f64,
    operation_success_prob: f64,
    total_ops: u32,
    successful_ops: u32,
    failed_ops: u32,
    tx: Sender<ProtocolMessage>,
    rx: Receiver<ProtocolMessage>,
}

///
/// Participant
/// Implementation of participant for the 2PC protocol
/// Required:
/// 1. new -- Constructor
/// 2. pub fn report_status -- Reports number of committed/aborted/unknown for each participant
/// 3. pub fn protocol() -- Implements participant side protocol for 2PC
///
impl Participant {

    ///
    /// new()
    ///
    /// Return a new participant, ready to run the 2PC protocol with the coordinator.
    ///
    /// HINT: You may want to pass some channels or other communication
    ///       objects that enable coordinator->participant and participant->coordinator
    ///       messaging to this constructor.
    /// HINT: You may want to pass some global flags that indicate whether
    ///       the protocol is still running to this constructor. There are other
    ///       ways to communicate this, of course.
    ///
    pub fn new(
        id_str: String,
        log_path: String,
        r: Arc<AtomicBool>,
        send_success_prob: f64,
        operation_success_prob: f64,
        tx: Sender<ProtocolMessage>,
        rx: Receiver<ProtocolMessage>,
        total_ops: u32,
    ) -> Participant {

        Participant {
            id_str: id_str,
            state: ParticipantState::Quiescent,
            log: oplog::OpLog::new(log_path),
            running: r,
            send_success_prob: send_success_prob,
            operation_success_prob: operation_success_prob,
            tx,
            rx,
            total_ops,
            successful_ops: 0,
            failed_ops: 0,
        }
    }

    ///
    /// send()
    /// Send a protocol message to the coordinator. This can fail depending on
    /// the success probability. For testing purposes, make sure to not specify
    /// the -S flag so the default value of 1 is used for failproof sending.
    ///
    /// HINT: You will need to implement the actual sending
    ///
    pub fn send(&mut self, pm: ProtocolMessage) {
        let x: f64 = random();
        if x <= self.send_success_prob {
            self.tx.send(pm).unwrap();
        } else {
            // DO Nothing
        }
    }

    ///
    /// perform_operation
    /// Perform the operation specified in the 2PC proposal,
    /// with some probability of success/failure determined by the
    /// command-line option success_probability.
    ///
    /// HINT: The code provided here is not complete--it provides some
    ///       tracing infrastructure and the probability logic.
    ///       Your implementation need not preserve the method signature
    ///       (it's ok to add parameters or return something other than
    ///       bool if it's more convenient for your design).
    ///
    pub fn perform_operation(&mut self, coord_msg: &ProtocolMessage) {
        trace!("{}::current state={:?}, message={:?}",self.id_str, self.state, coord_msg.mtype);
        match coord_msg.mtype {
            MessageType::CoordinatorPropose => {
                assert!(self.state == ParticipantState::Quiescent);
            },
            MessageType::CoordinatorCommit
            | MessageType::CoordinatorAbort => {
                assert!(self.state == ParticipantState::AwaitingGlobalDecision
                    || self.state == ParticipantState::Quiescent);
            },
            _ => panic!("Unknown message {:?}", coord_msg.mtype),
        }

        trace!("{}::Performing operation {}", self.id_str.clone(), coord_msg.txid);
        let x: f64 = random();
        if x <= self.operation_success_prob {
            match coord_msg.mtype {
                MessageType::CoordinatorPropose => {
                    if self.state == ParticipantState::Quiescent {
                        self.log.append_msg_as_type(coord_msg, MessageType::ParticipantVoteCommit);
                        self.state = ParticipantState::VotedCommit;
                        self.state = ParticipantState::AwaitingGlobalDecision;
                    }
                    trace!("{}::Voting commit {}, state: {:?}", self.id_str.clone(), coord_msg.txid, self.state);
                    self.send(coord_msg.own_as_type(self.id_str.clone(), MessageType::ParticipantVoteCommit));
                },
                MessageType::CoordinatorCommit => {
                    self.send(coord_msg.own_as_type(self.id_str.clone(), MessageType::ClientResultCommit));
                    if self.state == ParticipantState::AwaitingGlobalDecision {
                        self.log.append_msg_as_type(coord_msg, MessageType::CoordinatorCommit);
                        self.successful_ops += 1;
                        self.state = ParticipantState::Quiescent;
                    }
                    trace!("{}::committed, state: {:?}", self.id_str.clone(), self.state);
                },
                MessageType::CoordinatorAbort => {
                    self.send(coord_msg.own_as_type(self.id_str.clone(), MessageType::ClientResultAbort));
                    if self.state == ParticipantState::AwaitingGlobalDecision {
                        self.failed_ops += 1;
                        self.log.append_msg_as_type(coord_msg, MessageType::CoordinatorAbort);
                        self.state = ParticipantState::Quiescent;
                    }
                    trace!("{}::aborted, state: {:?}", self.id_str.clone(), self.state);
                },
                _ => panic!("Unexpected message type {:?} in participant", coord_msg.mtype),
            }
        } else {
            match coord_msg.mtype {
                MessageType::CoordinatorPropose => {
                    if self.state == ParticipantState::Quiescent {
                        self.log.append_msg_as_type(coord_msg, MessageType::ParticipantVoteAbort);
                        self.state = ParticipantState::VotedAbort;
                        self.state = ParticipantState::AwaitingGlobalDecision;
                    }
                    trace!("{}::Voting abort {}, state: {:?}", self.id_str.clone(), coord_msg.txid, self.state);
                    self.send(coord_msg.own_as_type(self.id_str.clone(), MessageType::ParticipantVoteAbort));
                },
                MessageType::CoordinatorCommit => {
                    self.send(coord_msg.own_as_type(self.id_str.clone(), MessageType::CoordinatorExit)); // i.e. not commit
                    trace!("{}::commit failed, state: {:?}", self.id_str.clone(), self.state);
                },
                MessageType::CoordinatorAbort => {
                    self.send(coord_msg.own_as_type(self.id_str.clone(), MessageType::CoordinatorExit)); // i.e. not abort
                    trace!("{}::abort failed, state: {:?}", self.id_str.clone(), self.state);
                },
                _ => panic!("Unexpected message type {:?} in participant", coord_msg.mtype),
            }
        }
    }

    ///
    /// report_status()
    /// Report the abort/commit/unknown status (aggregate) of all transaction
    /// requests made by this coordinator before exiting.
    ///
    pub fn report_status(&mut self) {
        // TODO: Collect actual stats
        let successful_ops: u32 = self.successful_ops;
        let failed_ops: u32 = self.failed_ops;
        let unknown_ops: u32 = self.total_ops - successful_ops - failed_ops;

        println!("{:16}:\tCommitted: {:6}\tAborted: {:6}\tUnknown: {:6}", self.id_str.clone(), successful_ops, failed_ops, unknown_ops);
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
    /// protocol()
    /// Implements the participant side of the 2PC protocol
    /// HINT: If the simulation ends early, don't keep handling requests!
    /// HINT: Wait for some kind of exit signal before returning from the protocol!
    ///
    pub fn protocol(&mut self) {
        trace!("{}::Beginning protocol", self.id_str.clone());

        loop {
            let msg: ProtocolMessage = self.rx.recv().unwrap();

            match msg.mtype {
                MessageType::CoordinatorPropose
                | MessageType::CoordinatorCommit
                | MessageType::CoordinatorAbort => {
                    self.perform_operation(&msg);
                },
                MessageType::CoordinatorExit => { break },
                _ => panic!("{}::Received unknown message type: {:?}", self.id_str.clone(), msg.mtype),
            }
        }

        // self.wait_for_exit_signal();
        self.report_status();
    }
}
