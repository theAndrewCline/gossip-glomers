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
        key: Option<String>,
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
    Add {
        msg_id: u32,
        delta: u32,
    },
    AddOk {
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

struct Node<'a> {
    id: String,
    neighbors: Vec<String>,
    next_msg_id: Option<u32>,
    messages: Vec<u32>,
    output: StdoutLock<'a>,
}

impl Node<'_> {
    fn send_message(&mut self, message: Message) -> () {
        serde_json::to_writer(&mut self.output, &message).expect("Failed to write JSON");

        self.output.write(b"\n").expect("Failed to write newline");
        self.output.flush().expect("Failed to flush");

        self.increase_msg_id();
    }

    fn increase_msg_id(&mut self) -> () {
        self.next_msg_id = Some(self.next_msg_id.unwrap_or(0) + 1);
    }

    fn step(&mut self, message: Message) -> () {
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

                self.send_message(reply);
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

                self.send_message(reply);
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

                self.send_message(reply);
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

                self.send_message(reply);
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

                self.send_message(reply);
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

                self.send_message(reply);
            }
            MessageBody::TopologyOk { .. } => {}

            MessageBody::Add { msg_id, .. } => {
                let reply = Message {
                    src: self.id.clone(),
                    dest: message.src,
                    body: MessageBody::AddOk {
                        msg_id: self.next_msg_id.unwrap_or(0),
                        in_reply_to: msg_id,
                    },
                };

                self.send_message(reply);
            }
            MessageBody::AddOk { .. } => {}
        }
    }
}

fn main() {
    let stdio = io::stdin().lock();
    let stdout = io::stdout().lock();

    let inputs = serde_json::Deserializer::from_reader(stdio).into_iter::<Message>();

    let mut state = Node {
        id: String::new(),
        neighbors: Vec::new(),
        next_msg_id: None,
        messages: Vec::new(),
        output: stdout,
    };

    for input in inputs {
        let message = input.expect("Failed to parse JSON");

        state.step(message);
    }
}
