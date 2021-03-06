#[macro_use]
extern crate crossbeam;
#[macro_use]
extern crate serde;

use crossbeam::channel;

use bincode::{deserialize, serialize_into};

use std::io;
use std::net::{SocketAddr, ToSocketAddrs, UdpSocket};
use std::thread;
use std::thread::JoinHandle;

mod kad;
use kad::*;

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
        let (send_tx, send_rx) = channel::unbounded();

        let mut kad = Kad::new(send_tx);

        // This channel is bounded so a huge inrush of packets doesn't consume unbounded memory
        // Right now it's a zero-capacity channel so it's effectively giving us the ability to
        // select on the socket and the command channel at the same time.
        let (recv_tx, recv_rx) = channel::bounded(0);

        let sender: JoinHandle<io::Result<()>> = thread::Builder::new().spawn(move || {
            let mut buf = Vec::new();
            while let Ok((pack, peer)) = send_rx.recv() {
                buf.clear();
                serialize_into(&mut buf, &pack).unwrap();
                let _ = send_sock.send_to(&buf, peer)?;
                eprintln!("Sent {:?} to {}", pack, peer);
            }
            Ok(())
        })?;

        let _recver: JoinHandle<io::Result<()>> = thread::Builder::new().spawn(move || {
            let mut buf = vec![0; 1 << 16]; // Maximum size of a UDP datagram
            loop {
                let (size, peer) = recv_sock.recv_from(&mut buf)?;
                if let Ok(pack) = deserialize(&buf[..size]) {
                    eprintln!("Received {:?} from {}", pack, peer);

                    if recv_tx.send((pack, peer)).is_err() {
                        return Ok(());
                    }
                }
            }
        })?;

        let worker = thread::Builder::new().spawn(move || loop {
            select! {
                recv(cmd_rx) -> cmd => {
                    if kad.handle_command(cmd.unwrap()) {
                        break
                    }
                }
                recv(recv_rx) -> packet => {
                    let (packet, peer) = packet.unwrap();
                    kad.handle_packet(packet, peer);
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
        }
    }

    pub fn shutdown(self) {
        self.command.send(Command::Shutdown).unwrap();
        self.worker.join().unwrap();
        self.sender.join().unwrap().unwrap();
        // Don't wait on recver, since it will never die until it gets a packet and discovers the broken channel
        //self.recver.join().unwrap().unwrap();
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }
}
