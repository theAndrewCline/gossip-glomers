use serde::{Deserialize, Serialize};
use std::io::{self, StdoutLock, Write};
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
    Generate {},
    GenerateOk {
        id: String,
    },
}

#[derive(Serialize, Deserialize, Debug)]
struct Message {
    src: String,
    dest: String,
    body: MessageBody,
}

struct Node {
    next_msg_id: Option<u32>,
}

impl Node {
    fn step(&mut self, message: Message, output: &mut StdoutLock) -> () {
        match message.body {
            MessageBody::Init { msg_id, .. } => {
                self.next_msg_id = Some(msg_id + 1);

                let reply = Message {
                    src: message.dest,
                    dest: message.src,
                    body: MessageBody::InitOk {
                        msg_id,
                        in_reply_to: msg_id,
                    },
                };

                serde_json::to_writer(&mut *output, &reply).unwrap();

                output.write(b"\n").unwrap();
                output.flush().unwrap();

                self.next_msg_id = Some(self.next_msg_id.unwrap() + 1);
            }

            MessageBody::InitOk { .. } => {}

            MessageBody::Echo { echo, msg_id } => {
                let reply = Message {
                    src: message.dest,
                    dest: message.src,
                    body: MessageBody::EchoOk {
                        msg_id,
                        in_reply_to: msg_id,
                        echo,
                    },
                };

                serde_json::to_writer(&mut *output, &reply).unwrap();

                output.write(b"\n").unwrap();
                output.flush().unwrap();

                self.next_msg_id = Some(self.next_msg_id.unwrap() + 1);
            }

            MessageBody::EchoOk { .. } => {}

            MessageBody::Generate {} => {
                let reply = Message {
                    src: message.dest,
                    dest: message.src,
                    body: MessageBody::GenerateOk {
                        id: Ulid::new().to_string(),
                    },
                };

                serde_json::to_writer(&mut *output, &reply).unwrap();

                output.write(b"\n").unwrap();
                output.flush().unwrap();

                self.next_msg_id = Some(self.next_msg_id.unwrap() + 1);
            }

            MessageBody::GenerateOk { .. } => {}
        }
    }
}

fn main() {
    let stdio = io::stdin().lock();
    let mut stdout = io::stdout().lock();

    let inputs = serde_json::Deserializer::from_reader(stdio).into_iter::<Message>();

    let mut state = Node { next_msg_id: None };

    for input in inputs {
        let message = input.expect("Failed to parse JSON");

        state.step(message, &mut stdout);
    }
}
