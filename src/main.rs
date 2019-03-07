#[macro_use]
extern crate crossbeam;

extern crate bincode;
#[macro_use]
extern crate serde;

use crossbeam::channel;

use bincode::{deserialize, serialize_into};

use std::env;
use std::io;
use std::net::{SocketAddr, ToSocketAddrs, UdpSocket};
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

#[derive(Serialize, Deserialize, Debug)]
enum Payload {
    Ping,
    Pong,
}

impl Payload {
    fn is_response(&self) -> bool {
        match self {
            Payload::Pong => true,
            _ => false,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct Packet {
    seq_num: u64,
    payload: Payload,
}

#[derive(Debug)]
enum Command {
    Shutdown,
    Ping(SocketAddr),
}

pub struct Dht {
    addr: SocketAddr,
    command: channel::Sender<Command>,

    worker: JoinHandle<()>,
    sender: JoinHandle<io::Result<()>>,
    //recver: JoinHandle<Result<()>>,
}

impl Dht {
    pub fn start<A: ToSocketAddrs>(socket: A) -> io::Result<Dht> {
        let send_sock = UdpSocket::bind(socket)?;
        let recv_sock = send_sock.try_clone()?;

        let socket = send_sock.local_addr().unwrap();

        let (cmd_tx, cmd_rx) = channel::unbounded();
        let (send_tx, send_rx): (_, channel::Receiver<(Packet, SocketAddr)>) = channel::unbounded();
        let (recv_tx, recv_rx): (_, channel::Receiver<(Packet, SocketAddr)>) = channel::bounded(10);

        let sender: JoinHandle<io::Result<()>> = thread::Builder::new().spawn(move || {
            let mut buf = Vec::new();
            while let Ok((pack, peer)) = send_rx.recv() {
                buf.clear();
                serialize_into(&mut buf, &pack).unwrap();
                let _ = send_sock.send_to(&buf, peer)?;
            }
            Ok(())
        })?;

        let _recver: JoinHandle<io::Result<()>> = thread::Builder::new().spawn(move || {
            let mut buf = vec![0; 512];
            loop {
                let (size, peer) = recv_sock.recv_from(&mut buf)?;
                if let Ok(pack) = deserialize(&buf[..size]) {
                    if recv_tx.send((pack, peer)).is_err() {
                        return Ok(());
                    }
                }
            }
        })?;

        let worker = thread::Builder::new().spawn(move || loop {
            select! {
                recv(cmd_rx) -> cmd => {
                    match cmd.unwrap() {
                        Command::Shutdown => break,
                        Command::Ping(peer) => send_tx.send((Packet {
                            seq_num: 0,
                            payload: Payload::Ping,
                        }, peer)).unwrap(),
                    }
                }
                recv(recv_rx) -> packet => {
                    let (packet, peer) = packet.unwrap();
                    match packet.payload {
                        Payload::Ping => send_tx.send((Packet {
                            seq_num: packet.seq_num,
                            payload: Payload::Pong,
                        }, peer)).unwrap(),
                        _ => ()
                    }
                }
            }
        })?;

        Ok(Dht {
            addr: socket,
            command: cmd_tx,

            worker: worker,
            sender: sender,
            //recver: recver,
        })
    }

    pub fn bootstrap<A: ToSocketAddrs>(&mut self, peers: A) {
        for peer in peers.to_socket_addrs().unwrap() {
            self.command.send(Command::Ping(peer)).ok();
            eprintln!("Queued ping for {}", peer);
        }
    }

    pub fn shutdown(self) {
        self.command.send(Command::Shutdown).unwrap();
        self.worker.join().unwrap();
        self.sender.join().unwrap().unwrap();
        // Don't wait on recver, since it will never die until it gets a packet and discovers the broken channel
        //self.recver.join().unwrap().unwrap();
    }
}

fn main() {
    let mut dht = Dht::start("[::]:0").unwrap();
    println!("Bound to {}", dht.addr);
    env::args().skip(1).for_each(|a| dht.bootstrap(a));
    thread::sleep(Duration::from_millis(10000));
    dht.shutdown();
}