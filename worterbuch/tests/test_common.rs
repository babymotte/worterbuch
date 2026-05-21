use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs,
    io::{self, BufRead, BufReader, BufWriter, Lines, Write},
    net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpStream},
    path::Path,
    process::{Child, Command, Stdio},
    time::Duration,
};
use worterbuch_common::{
    ClientMessage, ErrorCode, Get, Key, KeyValuePair, ServerMessage, State, StateEvent,
    TransactionId,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IntegrationTest {
    setup: Box<[ClientMessage]>,
    assert: Box<[IntegrationTestAssertion]>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IntegrationTestEnvironment {
    env: HashMap<String, String>,
}

impl IntegrationTestEnvironment {
    pub fn data_dir(&self) -> &str {
        self.env
            .get("WORTERBUCH_DATA_DIR")
            .map(|s| s.as_str())
            .unwrap_or("./data")
    }

    pub fn tcp_port(&self) -> u16 {
        self.env
            .get("WORTERBUCH_TCP_SERVER_PORT")
            .and_then(|s| s.parse().ok())
            .unwrap_or(8081)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum IntegrationTestAssertion {
    Equals(Box<[KeyValuePair]>),
    NotPresent(Box<[Key]>),
}

pub struct TestRunner {
    server_setup_path: std::path::PathBuf,
    test_setup_path: std::path::PathBuf,
}

pub struct Client {
    writer: BufWriter<TcpStream>,
    reader: Lines<BufReader<TcpStream>>,
    id: String,
}

pub struct ActiveTestRunner {
    server_setup_path: std::path::PathBuf,
    test_setup_path: std::path::PathBuf,
    process: Child,
    server_setup: IntegrationTestEnvironment,
}

impl ActiveTestRunner {
    pub fn run_test(&self, client: &mut Client, test: &IntegrationTest) {
        for setup_step in &test.setup {
            let request = serde_json::to_string(setup_step)
                .expect("failed to serialize client message as JSON");
            eprintln!("request: {request}");
            client
                .writer
                .write_all((request + "\n").as_bytes())
                .expect("failed to write client message to TCP stream");
            client.writer.flush().expect("failed to flush TCP stream");
            let response = client
                .reader
                .next()
                .expect("failed to read response from TCP stream")
                .expect("failed to read line from TCP stream");
            eprintln!("response: {response}");
        }

        for assertion in &test.assert {
            match assertion {
                IntegrationTestAssertion::Equals(key_value_pairs) => {
                    assert_equals(client, key_value_pairs)
                }
                IntegrationTestAssertion::NotPresent(items) => assert_not_present(client, items),
            }
        }
    }

    pub fn shutdown_wb(mut self) -> TestRunner {
        drop(self.process.stdin.take());

        let status = self.process.wait().expect("failed to wait for wb process");
        assert!(
            status.success(),
            "worterbuch process exited with a non-zero status"
        );

        TestRunner {
            server_setup_path: self.server_setup_path.clone(),
            test_setup_path: self.test_setup_path.clone(),
        }
    }

    pub fn kill_wb(mut self) -> TestRunner {
        self.process.kill().expect("failed to kill wb process");

        self.process.wait().expect("failed to wait for wb process");

        TestRunner {
            server_setup_path: self.server_setup_path.clone(),
            test_setup_path: self.test_setup_path.clone(),
        }
    }
    pub fn start_client_test(&self) -> (Client, Box<[IntegrationTest]>) {
        let port = self.server_setup.tcp_port();
        let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), port));

        let stream = connect(addr);

        let reader = BufReader::new(stream.try_clone().expect("failed to clone TCP stream"));
        let writer = BufWriter::new(stream);

        let mut lines = reader.lines();
        let welcome = lines
            .next()
            .expect("failed to read client ID from wb")
            .expect("failed to read line from TCP stream");
        let ServerMessage::Welcome(welcome) =
            serde_json::from_str(&welcome).expect("failed to parse welcome message as JSON")
        else {
            panic!("expected welcome message");
        };

        let client = Client {
            writer,
            reader: lines,
            id: welcome.client_id,
        };

        let tests = self.materialize_tests(&client.id);
        (client, tests)
    }

    fn materialize_tests(&self, client_id: &str) -> Box<[IntegrationTest]> {
        let test_setup =
            fs::read_to_string(&self.test_setup_path).expect("failed to read test setup data");
        let test_setup = test_setup.replace("{{client_id}}", client_id);

        let mut test_data = test_setup.replace("\"{{tid}}\"", "{{tid}}");
        let mut test_data_subst = "".into();

        let mut tid = 1;
        while test_data_subst != test_data {
            test_data_subst = test_data;
            test_data = test_data_subst.replacen("{{tid}}", &tid.to_string(), 1);
            tid += 1;
        }

        serde_json::from_str(&test_data_subst).expect("failed to parse test setup as JSON")
    }
}

fn assert_equals(client: &mut Client, key_value_pairs: &[KeyValuePair]) {
    for (tid, KeyValuePair { key, value }) in key_value_pairs.iter().enumerate() {
        let request = serde_json::to_string(&ClientMessage::Get(Get {
            transaction_id: tid as TransactionId,
            key: key.clone(),
        }))
        .expect("failed to serialize client message as JSON");
        eprintln!("request: {request}");
        client
            .writer
            .write_all((request + "\n").as_bytes())
            .expect("failed to write client message to TCP stream");
        client.writer.flush().expect("failed to flush TCP stream");
        let response = client
            .reader
            .next()
            .expect("failed to read response from TCP stream")
            .expect("failed to read line from TCP stream");
        eprintln!("response: {response}");
        let response: ServerMessage =
            serde_json::from_str(&response).expect("failed to parse server message as JSON");

        let ServerMessage::State(State {
            transaction_id,
            event,
        }) = response
        else {
            panic!("expected State message, but got {:?}", response);
        };

        assert_eq!(
            tid as TransactionId, transaction_id,
            "expected response for transaction ID {tid} but got response for transaction ID {}",
            transaction_id
        );

        let StateEvent::Value(v) = event else {
            panic!("expected Value event, but got {:?}", event);
        };

        assert_eq!(
            value, &v,
            "expected value {:?} for key {:?} but got {:?}",
            value, key, v
        );
    }
}

fn assert_not_present(client: &mut Client, items: &[String]) {
    for (tid, key) in items.iter().enumerate() {
        let request = serde_json::to_string(&ClientMessage::Get(Get {
            transaction_id: tid as TransactionId,
            key: key.clone(),
        }))
        .expect("failed to serialize client message as JSON");
        eprintln!("request: {request}");
        client
            .writer
            .write_all((request + "\n").as_bytes())
            .expect("failed to write client message to TCP stream");
        client.writer.flush().expect("failed to flush TCP stream");
        let response = client
            .reader
            .next()
            .expect("failed to read response from TCP stream")
            .expect("failed to read line from TCP stream");
        eprintln!("response: {response}");

        let response: ServerMessage =
            serde_json::from_str(&response).expect("failed to parse server message as JSON");

        assert_eq!(
            Some(tid as TransactionId),
            response.transaction_id(),
            "expected response for transaction ID {tid} but got response for transaction ID {:?}",
            response.transaction_id()
        );

        match response {
            ServerMessage::State(state) => {
                panic!(
                    "expected no value for key {:?} but got value {:?}",
                    items[tid], state.event
                );
            }
            ServerMessage::Err(e) => {
                if e.error_code != ErrorCode::NoSuchValue {
                    panic!(
                        "expected NoSuchValue error for key {:?} but got error {:?}",
                        items[tid], e
                    );
                }
            }
            _ => panic!("expected State or Err message, but got {:?}", response),
        }
    }
}

impl Drop for ActiveTestRunner {
    fn drop(&mut self) {
        if self.process.kill().is_err() {
            eprintln!("worterbuch process was shut down forcefully");
        }
    }
}

impl TestRunner {
    pub fn new(server_setup: impl AsRef<Path>, test_setup: impl AsRef<Path>) -> Self {
        Self {
            server_setup_path: server_setup.as_ref().to_owned(),
            test_setup_path: test_setup.as_ref().to_owned(),
        }
    }

    pub fn start_wb(self, clean_persistence_data: bool) -> ActiveTestRunner {
        let server_setup =
            fs::read_to_string(&self.server_setup_path).expect("failed to read server setup data");

        let server_setup: IntegrationTestEnvironment =
            serde_json::from_str(&server_setup).expect("failed to parse server setup as JSON");

        let process = start_wb(&server_setup, clean_persistence_data);

        ActiveTestRunner {
            server_setup_path: self.server_setup_path,
            test_setup_path: self.test_setup_path,
            server_setup,
            process,
        }
    }
}

fn start_wb(setup: &IntegrationTestEnvironment, clean_persistence_data: bool) -> Child {
    let data_dir = setup.data_dir();

    if clean_persistence_data {
        if let Err(e) = fs::remove_dir_all(data_dir) {
            if e.kind() != io::ErrorKind::NotFound {
                panic!("failed to clear data dir: {}", e);
            }
        }
    }
    fs::create_dir_all(data_dir).expect("failed to create data dir");

    let child = Command::new("../target/debug/worterbuch")
        .envs(setup.env.iter())
        // makes sure wb is shut down when the test is done
        .stdin(Stdio::piped())
        .spawn()
        .expect("failed to start wb");

    // child.

    // let stdout = child.stdout.expect("failed to capture stdout");

    child
}

fn connect(addr: SocketAddr) -> TcpStream {
    let mut err = None;
    for i in 0..5 {
        match TcpStream::connect(addr) {
            Ok(stream) => return stream,
            Err(e) => {
                err = Some(e);
                std::thread::sleep(Duration::from_millis(100 * 2u64.pow(i)));
            }
        }
    }
    panic!("failed to connect to wb TCP server: {:?}", err);
}
