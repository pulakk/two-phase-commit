#[macro_use]
extern crate log;
extern crate stderrlog;
extern crate clap;
extern crate ctrlc;
extern crate ipc_channel;
extern crate serde;
extern crate nix;
use std::env;
use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::process::{Child,Command};
use client::Client;
use ipc_channel::ipc::IpcSender as Sender;
use ipc_channel::ipc::IpcReceiver as Receiver;
use ipc_channel::ipc::IpcOneShotServer;
use ipc_channel::ipc::channel;
pub mod message;
pub mod oplog;
pub mod coordinator;
pub mod participant;
pub mod client;
pub mod checker;
pub mod tpcoptions;
use message::ProtocolMessage;
use participant::Participant;

///
/// pub fn spawn_child_and_connect(child_opts: &mut tpcoptions::TPCOptions) -> (std::process::Child, Sender<ProtocolMessage>, Receiver<ProtocolMessage>)
///
///     child_opts: CLI options for child process
///
/// 1. Set up IPC
/// 2. Spawn a child process using the child CLI options
/// 3. Do any required communication to set up the parent / child communication channels
/// 4. Return the child process handle and the communication channels for the parent
///
/// HINT: You can change the signature of the function if necessary
///
fn spawn_child_and_connect(child_opts: &mut tpcoptions::TPCOptions, ipc_one_shot_server: IpcOneShotServer<(Sender<ProtocolMessage>, Receiver<ProtocolMessage>)>) -> (Child, Sender<ProtocolMessage>, Receiver<ProtocolMessage>) {
    let child = Command::new(env::current_exe().unwrap())
        .args(child_opts.as_vec())
        .spawn()
        .expect("Failed to execute child process");

    let (_, (tx_from_child, rx_to_child)) = ipc_one_shot_server.accept().unwrap();

    (child, tx_from_child, rx_to_child)
}

///
/// pub fn connect_to_coordinator(opts: &tpcoptions::TPCOptions) -> (Sender<ProtocolMessage>, Receiver<ProtocolMessage>)
///
///     opts: CLI options for this process
///
/// 1. Connect to the parent via IPC
/// 2. Do any required communication to set up the parent / child communication channels
/// 3. Return the communication channels for the child
///
/// HINT: You can change the signature of the function if necessasry
///
fn connect_to_coordinator(opts: &tpcoptions::TPCOptions) -> (Sender<ProtocolMessage>, Receiver<ProtocolMessage>) {
    let one_shot_tx = Sender::connect(opts.ipc_path.clone()).unwrap();

    let (tx_from_coord, rx_from_coord) = channel::<ProtocolMessage>().unwrap();
    let (tx_to_coord, rx_to_coord) = channel::<ProtocolMessage>().unwrap();
    one_shot_tx.send((tx_from_coord, rx_to_coord)).unwrap();

    (tx_to_coord, rx_from_coord)
}

pub fn run_check(mut opts: tpcoptions::TPCOptions) {
    opts.mode = "check".to_string();

    let mut child = Command::new(env::current_exe().unwrap())
        .args(opts.as_vec())
        .spawn()
        .expect("Failed to execute child process");

    child.wait().expect("Error in waiting for check process");
}

///
/// pub fn run(opts: &tpcoptions:TPCOptions, running: Arc<AtomicBool>)
///     opts: An options structure containing the CLI arguments
///     running: An atomically reference counted (ARC) AtomicBool(ean) that is
///         set to be false whenever Ctrl+C is pressed
///
/// 1. Creates a new coordinator
/// 2. Spawns and connects to new clients processes and then registers them with
///    the coordinator
/// 3. Spawns and connects to new participant processes and then registers them
///    with the coordinator
/// 4. Starts the coordinator protocol
/// 5. Wait until the children finish execution
///
fn run(opts: & tpcoptions::TPCOptions, running: Arc<AtomicBool>) {
    let coord_log_path = format!("{}//{}", opts.log_path, "coordinator.log");
    let mut coord = coordinator::Coordinator::new(coord_log_path, &running, opts.num_requests);

    println!("Coord Pid = {}", std::process::id());

    let mut clients = (0..opts.num_clients).map(|i| {
        let (server, ipc_path) = IpcOneShotServer::new().unwrap();
        let mut client_opts = opts.clone();
        client_opts.ipc_path = ipc_path;
        client_opts.num = i;
        client_opts.mode = "client".to_string();
        let (child, tx, rx) = spawn_child_and_connect(&mut client_opts, server);
        coord.client_join(tx, rx);

        child
    }).collect::<Vec<Child>>();

    let mut participants = (0..opts.num_participants).map(|i| {
        let (server, ipc_path) = IpcOneShotServer::new().unwrap();
        let mut participant_opts = opts.clone();
        participant_opts.ipc_path = ipc_path;
        participant_opts.num = i;
        participant_opts.mode = "participant".to_string();
        let (child, tx, rx) = spawn_child_and_connect(&mut participant_opts, server);
        coord.participant_join(tx, rx);

        child
    }).collect::<Vec<Child>>();

    coord.protocol();

    for child in &mut clients {
        child.wait().expect("Failed waiting for client");
    }
    for child in &mut participants {
        child.wait().expect("Failed waiting for participant");
    };

    run_check(opts.clone());
}

///
/// pub fn run_client(opts: &tpcoptions:TPCOptions, running: Arc<AtomicBool>)
///     opts: An options structure containing the CLI arguments
///     running: An atomically reference counted (ARC) AtomicBool(ean) that is
///         set to be false whenever Ctrl+C is pressed
///
/// 1. Connects to the coordinator to get tx/rx
/// 2. Constructs a new client
/// 3. Starts the client protocol
///
fn run_client(opts: & tpcoptions::TPCOptions, running: Arc<AtomicBool>) {
    let (tx, rx) = connect_to_coordinator(opts);
    let mut client = Client::new(format!("client_{}", opts.num), running, tx, rx);
    client.protocol(opts.num_requests);
}

///
/// pub fn run_participant(opts: &tpcoptions:TPCOptions, running: Arc<AtomicBool>)
///     opts: An options structure containing the CLI arguments
///     running: An atomically reference counted (ARC) AtomicBool(ean) that is
///         set to be false whenever Ctrl+C is pressed
///
/// 1. Connects to the coordinator to get tx/rx
/// 2. Constructs a new participant
/// 3. Starts the participant protocol
///
fn run_participant(opts: & tpcoptions::TPCOptions, running: Arc<AtomicBool>) {
    let participant_id_str = format!("participant_{}", opts.num);
    let participant_log_path = format!("{}//{}.log", opts.log_path, participant_id_str);

    let (tx, rx) = connect_to_coordinator(opts);
    let mut participant = Participant::new(
        participant_id_str,
        participant_log_path,
        running,
        opts.send_success_probability,
        opts.operation_success_probability,
        tx,
        rx,
        opts.num_requests * opts.num_clients,
    );

    participant.protocol();
}

fn main() {
    // Parse CLI arguments
    let opts = tpcoptions::TPCOptions::new();
    // Set-up logging and create OpLog path if necessary
    stderrlog::new()
            .module(module_path!())
            .quiet(false)
            .timestamp(stderrlog::Timestamp::Millisecond)
            .verbosity(opts.verbosity)
            .init()
            .unwrap();
    match fs::create_dir_all(opts.log_path.clone()) {
        Err(e) => error!("Failed to create log_path: \"{:?}\". Error \"{:?}\"", opts.log_path, e),
        _ => (),
    }

    // Set-up Ctrl-C / SIGINT handler
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    let m = opts.mode.clone();
    ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
        if m == "run" {
            print!("\n");
        }
    }).expect("Error setting signal handler!");

    // Execute main logic
    match opts.mode.as_ref() {
        "run" => run(&opts, running),
        "client" => run_client(&opts, running),
        "participant" => run_participant(&opts, running),
        "check" => checker::check_last_run(opts.num_clients, opts.num_requests, opts.num_participants, &opts.log_path),
        _ => panic!("Unknown mode"),
    }
}
