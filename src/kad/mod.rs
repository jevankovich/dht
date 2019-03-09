extern crate bincode;
extern crate crossbeam;

use crossbeam::channel;
use std::net::SocketAddr;

mod kbucket;
use kbucket::*;

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
pub struct Packet {
    seq_num: u64,
    payload: Payload,
}

#[derive(Debug)]
pub enum Command {
    Shutdown,
    Ping(SocketAddr),
}

pub struct Kad {
    send: channel::Sender<(Packet, SocketAddr)>,

    known_peers: KBuckets,
}

impl Kad {
    pub fn new(send: channel::Sender<(Packet, SocketAddr)>) -> Kad {
        Kad {
            send: send,
            known_peers: KBuckets::new(),
        }
    }

    pub fn handle_packet(&mut self, pack: Packet, peer: SocketAddr) {
        match pack.payload {
            Payload::Ping => self
                .send
                .send((
                    Packet {
                        seq_num: pack.seq_num,
                        payload: Payload::Pong,
                    },
                    peer,
                ))
                .unwrap(),
            _ => (),
        }
    }

    pub fn handle_command(&mut self, command: Command) -> bool {
        match command {
            Command::Shutdown => return true,
            Command::Ping(peer) => self
                .send
                .send((
                    Packet {
                        seq_num: 0,
                        payload: Payload::Ping,
                    },
                    peer,
                ))
                .unwrap(),
        };

        false
    }
}
