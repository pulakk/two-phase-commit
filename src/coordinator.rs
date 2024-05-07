//!
//! coordinator.rs
//! Implementation of 2PC coordinator
//!
extern crate log;
extern crate stderrlog;
extern crate rand;
extern crate ipc_channel;

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use coordinator::ipc_channel::ipc::IpcSender as Sender;
use coordinator::ipc_channel::ipc::IpcReceiver as Receiver;

use message::MessageType;
use message::ProtocolMessage;
use oplog;

/// CoordinatorState
/// States for 2PC state machine
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CoordinatorState {
    Quiescent,
    ReceivedRequest,
    ProposalSent,
    ReceivedVotesAbort,
    ReceivedVotesCommit,
    SentGlobalDecision,
}

/// Coordinator
/// Struct maintaining state for coordinator
#[allow(dead_code)]
#[derive(Debug)]
pub struct Coordinator {
    state: CoordinatorState,
    running: Arc<AtomicBool>,
    log: oplog::OpLog,
    participants: Vec<(Sender<ProtocolMessage>, Receiver<ProtocolMessage>)>,
    clients: Vec<(Sender<ProtocolMessage>, Receiver<ProtocolMessage>)>,
    total_ops: u32,
    n_reqs: u32,
    successful_ops: u32,
    failed_ops: u32,
}

///
/// Coordinator
/// Implementation of coordinator functionality
/// Required:
/// 1. new -- Constructor
/// 2. protocol -- Implementation of coordinator side of protocol
/// 3. report_status -- Report of aggregate commit/abort/unknown stats on exit.
/// 4. participant_join -- What to do when a participant joins
/// 5. client_join -- What to do when a client joins
///
impl Coordinator {

    ///
    /// new()
    /// Initialize a new coordinator
    ///
    /// <params>
    ///     log_path: directory for log files --> create a new log there.
    ///     r: atomic bool --> still running?
    ///
    pub fn new(
        log_path: String,
        r: &Arc<AtomicBool>,
        total_ops: u32,
    ) -> Coordinator {

        Coordinator {
            state: CoordinatorState::Quiescent,
            log: oplog::OpLog::new(log_path),
            running: r.clone(),
            participants: vec![],
            clients: vec![],
            total_ops,
            n_reqs: 0,
            successful_ops: 0,
            failed_ops: 0,
        }
    }

    ///
    /// participant_join()
    /// Adds a new participant for the coordinator to keep track of
    ///
    /// HINT: Keep track of any channels involved!
    /// HINT: You may need to change the signature of this function
    ///
    pub fn participant_join(&mut self, tx: Sender<ProtocolMessage>, rx: Receiver<ProtocolMessage>) {
        assert!(self.state == CoordinatorState::Quiescent);

        self.participants.push((tx, rx));
    }

    ///
    /// client_join()
    /// Adds a new client for the coordinator to keep track of
    ///
    /// HINT: Keep track of any channels involved!
    /// HINT: You may need to change the signature of this function
    ///
    pub fn client_join(&mut self, tx: Sender<ProtocolMessage>, rx: Receiver<ProtocolMessage>) {
        assert!(self.state == CoordinatorState::Quiescent);

        self.clients.push((tx, rx));
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
        let unknown_ops: u32 = self.total_ops * self.clients.len() as u32 - successful_ops - failed_ops;

        println!("coordinator     :\tCommitted: {:6}\tAborted: {:6}\tUnknown: {:6}", successful_ops, failed_ops, unknown_ops);
    }

    fn all_reqs_done(&self) -> bool {
        self.n_reqs >= self.total_ops * self.clients.len() as u32
    }

    fn flush_rx(&self, rx: &Receiver<ProtocolMessage>) {
        loop {
            match rx.try_recv() {
                Ok(_) => {},
                Err(_) => return,
            }
        }
    }

    fn handle_client_req(&mut self, client_id: usize, client_msg: ProtocolMessage) {
        assert!(self.state == CoordinatorState::Quiescent);
        self.state = CoordinatorState::ReceivedRequest;
        self.n_reqs += 1;

        trace!("{}::handling client req {:?}", "coord", client_msg.txid);

        // propose to all participants
        for (tx, rx) in &self.participants {
            self.flush_rx(rx);
            tx.send(client_msg.as_type(MessageType::CoordinatorPropose)).unwrap()
        }
        self.state = CoordinatorState::ProposalSent;
        trace!("coord::state={:?}", self.state);

        // wait for responses
        let voted_for_commit = self.participants.iter().enumerate().all(|(i, (_, rx))| {
            // TODO: Convert to thread
            if i == 0 {
                match rx.try_recv_timeout(Duration::new(1, 0)) {
                    Ok(result) => result.mtype == MessageType::ParticipantVoteCommit,
                    Err(_) => false,
                }
            } else {
                match rx.try_recv() {
                    Ok(result) => result.mtype == MessageType::ParticipantVoteCommit,
                    Err(_) => false,
                }
                
            }
        });

        // process result
        self.state = if voted_for_commit { CoordinatorState::ReceivedVotesCommit }
            else { CoordinatorState::ReceivedVotesAbort };
        let decision_mtype = if voted_for_commit { MessageType::CoordinatorCommit }
            else { MessageType::CoordinatorAbort };
        let client_result_mtype = if voted_for_commit { MessageType::ClientResultCommit }
            else { MessageType::ClientResultAbort };

        // force commit / abort all participants
        for (tx, rx) in &self.participants {
            // TODO: Convert to thread
            loop {
                self.flush_rx(rx);
                tx.send(client_msg.as_type(decision_mtype)).unwrap();
                match rx.try_recv_timeout(Duration::new(1, 0)) {
                    Ok(result) => {
                        let participant_done = (voted_for_commit
                                && result.mtype == MessageType::ClientResultCommit)
                            || (!voted_for_commit
                                && result.mtype == MessageType::ClientResultAbort);
                        trace!("coord::voted for commit={}, {} tx={} result={:?}, done={}", voted_for_commit, result.senderid, result.txid, result.mtype, participant_done);
                        if participant_done { break; }
                    },
                    Err(_) => { trace!("coord::participant response timeout") },
                }
            }
        }
        self.state = CoordinatorState::SentGlobalDecision;
        trace!("coord::state={:?}", self.state);

        // commit / abort myself
        self.log.append_msg_as_type(&client_msg, decision_mtype);

        // respond to client
        let client_tx = &self.clients[client_id].0;
        client_tx.send(client_msg.as_type(client_result_mtype)).unwrap();
        if voted_for_commit { self.successful_ops += 1; }
        else { self.failed_ops += 1; }
        self.state = CoordinatorState::Quiescent;
        trace!("coord::state={:?}", self.state);
    }

    fn close_all_participants(&self) {
        for (tx, _) in &self.participants {
            let msg = ProtocolMessage::generate(
                MessageType::CoordinatorExit,
                String::new(),
                String::new(),
                0
            );
            tx.send(msg).unwrap();
        }
    }

    ///
    /// protocol()
    /// Implements the coordinator side of the 2PC protocol
    /// HINT: If the simulation ends early, don't keep handling requests!
    /// HINT: Wait for some kind of exit signal before returning from the protocol!
    ///
    pub fn protocol(&mut self) {
        loop {
            for client_id in 0..self.clients.len() {
                if self.all_reqs_done() { break };

                match self.clients[client_id].1.try_recv() {
                    Ok(client_req) => {self.handle_client_req(client_id, client_req)},
                    Err(_) => {},
                }
            }

            if self.all_reqs_done() { break };
        }

        self.close_all_participants();

        self.report_status();
    }
}
