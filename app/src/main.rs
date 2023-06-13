use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    io::{self, StdoutLock, Write},
};
use ulid::Ulid;

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum MessageBody {
    Init {
        msg_id: u32,
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk {
        msg_id: u32,
        in_reply_to: u32,
    },
    EchoOk {
        msg_id: u32,
        in_reply_to: u32,
        echo: String,
    },
    Echo {
        msg_id: u32,
        echo: String,
    },
    Generate {
        msg_id: u32,
    },
    GenerateOk {
        msg_id: u32,
        in_reply_to: u32,
        id: String,
    },
    Broadcast {
        msg_id: u32,
        #[serde(rename = "message")]
        msg: u32,
    },
    BroadcastOk {
        msg_id: u32,
        in_reply_to: u32,
    },
    Read {
        msg_id: u32,
    },
    ReadOk {
        msg_id: u32,
        in_reply_to: u32,
        messages: Vec<u32>,
    },
    Topology {
        msg_id: u32,
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk {
        msg_id: u32,
        in_reply_to: u32,
    },
}

#[derive(Serialize, Deserialize, Debug)]
struct Message {
    src: String,
    dest: String,
    body: MessageBody,
}

struct Node {
    id: String,
    neighbors: Vec<String>,
    next_msg_id: Option<u32>,
    messages: Vec<u32>,
}

impl Node {
    fn step(&mut self, message: Message, output: &mut StdoutLock) -> () {
        match message.body {
            MessageBody::Init {
                msg_id,
                node_id,
                node_ids,
            } => {
                self.next_msg_id = Some(msg_id + 1);
                self.id = node_id;
                self.neighbors = node_ids;

                let reply = Message {
                    src: self.id.clone(),
                    dest: message.src,
                    body: MessageBody::InitOk {
                        msg_id,
                        in_reply_to: msg_id,
                    },
                };

                serde_json::to_writer(&mut *output, &reply).expect("Failed to write JSON");

                output.write(b"\n").expect("Failed to write newline");
                output.flush().expect("Failed to flush");

                self.next_msg_id = Some(self.next_msg_id.unwrap_or(0) + 1);
            }

            MessageBody::InitOk { .. } => {}

            MessageBody::Echo { echo, msg_id } => {
                let reply = Message {
                    src: self.id.clone(),
                    dest: message.src,
                    body: MessageBody::EchoOk {
                        msg_id,
                        in_reply_to: msg_id,
                        echo,
                    },
                };

                serde_json::to_writer(&mut *output, &reply).expect("Failed to write JSON");

                output.write(b"\n").expect("Failed to write newline");
                output.flush().expect("Failed to flush");

                self.next_msg_id = Some(self.next_msg_id.unwrap_or(0) + 1);
            }

            MessageBody::EchoOk { .. } => {}

            MessageBody::Generate { msg_id } => {
                let reply = Message {
                    src: self.id.clone(),
                    dest: message.src,
                    body: MessageBody::GenerateOk {
                        id: Ulid::new().to_string(),
                        msg_id: self.next_msg_id.unwrap_or(0),
                        in_reply_to: msg_id,
                    },
                };

                serde_json::to_writer(&mut *output, &reply).expect("Failed to write JSON");

                output.write(b"\n").expect("Failed to write newline");
                output.flush().expect("Failed to flush");

                self.next_msg_id = Some(self.next_msg_id.unwrap_or(0) + 1);
            }

            MessageBody::GenerateOk { .. } => {}

            MessageBody::Broadcast { msg, msg_id } => {
                self.messages.push(msg);

                let reply = Message {
                    src: self.id.clone(),
                    dest: message.src,
                    body: MessageBody::BroadcastOk {
                        msg_id: self.next_msg_id.unwrap_or(0),
                        in_reply_to: msg_id,
                    },
                };

                serde_json::to_writer(&mut *output, &reply).expect("Failed to write JSON");

                output.write(b"\n").expect("Failed to write newline");
                output.flush().expect("Failed to flush");

                self.next_msg_id = Some(self.next_msg_id.unwrap_or(0) + 1);
            }
            MessageBody::BroadcastOk { .. } => {}

            MessageBody::Read { msg_id } => {
                let reply = Message {
                    src: self.id.clone(),
                    dest: message.src,
                    body: MessageBody::ReadOk {
                        msg_id: self.next_msg_id.unwrap_or(0),
                        in_reply_to: msg_id,
                        messages: self.messages.clone(),
                    },
                };

                serde_json::to_writer(&mut *output, &reply).expect("Failed to write JSON");

                output.write(b"\n").expect("Failed to write newline");
                output.flush().expect("Failed to flush");

                self.next_msg_id = Some(self.next_msg_id.unwrap_or(0) + 1);
            }
            MessageBody::ReadOk { .. } => {}

            MessageBody::Topology { topology, msg_id } => {
                let neighbors = topology.get(&self.id);

                match neighbors {
                    Some(neighbors) => {
                        self.neighbors = neighbors.clone();
                    }
                    None => {
                        self.neighbors = Vec::new();
                    }
                }

                let reply = Message {
                    src: self.id.clone(),
                    dest: message.src,
                    body: MessageBody::TopologyOk {
                        msg_id: self.next_msg_id.unwrap_or(0),
                        in_reply_to: msg_id,
                    },
                };

                serde_json::to_writer(&mut *output, &reply).expect("Failed to write JSON");

                output.write(b"\n").expect("Failed to write newline");
                output.flush().expect("Failed to flush");

                self.next_msg_id = Some(self.next_msg_id.unwrap_or(0) + 1);
            }
            MessageBody::TopologyOk { .. } => {}
        }
    }
}

fn main() {
    let stdio = io::stdin().lock();
    let mut stdout = io::stdout().lock();

    let inputs = serde_json::Deserializer::from_reader(stdio).into_iter::<Message>();

    let mut state = Node {
        id: String::new(),
        neighbors: Vec::new(),
        next_msg_id: None,
        messages: Vec::new(),
    };

    for input in inputs {
        let message = input.expect("Failed to parse JSON");

        state.step(message, &mut stdout);
    }
}
