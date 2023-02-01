use anyhow::Result;
use clap::{App, Arg, ArgMatches};
use serde::Serialize;
use worterbuch_client::{config::Config, Err, PState, ServerMessage as SM, State};

pub fn print_message(msg: &SM, json: bool) {
    match msg {
        SM::PState(msg) => print_pstate(&msg, json),
        SM::State(msg) => print_state(&msg, json),
        SM::Err(msg) => print_err(&msg, json),
        _ => (),
    }
}

fn print_pstate(msg: &PState, json: bool) {
    if json {
        print_msg_as_json(&msg);
    } else {
        println!("{msg}");
    }
}

fn print_state(msg: &State, json: bool) {
    if json {
        print_msg_as_json(&msg);
    } else {
        println!("{msg}");
    }
}

fn print_err(msg: &Err, json: bool) {
    if json {
        print_msg_as_json(&msg);
    } else {
        eprintln!("{msg}");
    }
}

fn print_msg_as_json(msg: impl Serialize) {
    match serde_json::to_string(&msg) {
        Ok(json) => println!("{json}"),
        Err(e) => {
            eprintln!("Error converting message to json: {e}");
        }
    }
}

pub fn app<'help>(
    name: &'help str,
    about: &'help str,
    args: Vec<Arg<'help>>,
) -> Result<(ArgMatches, String, String, u16, bool)> {
    dotenv::dotenv().ok();
    env_logger::init();
    let config = Config::new()?;

    let mut app = App::new(name)
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(about);
    for arg in default_args().iter().chain(args.iter()) {
        app = app.arg(arg)
    }

    let matches = app.get_matches();

    let default_proto = config.proto;
    let default_host_addr = config.host_addr;
    let default_port = config.port;

    let proto = default_proto;
    let host_addr = matches
        .get_one::<String>("ADDR")
        .map(ToOwned::to_owned)
        .unwrap_or(default_host_addr);
    let port = matches
        .get_one::<String>("PORT")
        .and_then(|p| p.parse().ok())
        .unwrap_or(default_port);

    let json = matches.is_present("JSON");

    Ok((matches, proto, host_addr, port, json))
}

fn default_args<'help>() -> Vec<Arg<'help>> {
    let args = vec![
        Arg::with_name("ADDR")
            .short('a')
            .long("addr")
            .help("The address of the Wörterbuch server. When omitted, the value of the env var WORTERBUCH_HOST_ADDRESS will be used. If that is not set, 127.0.0.1 will be used.")
            .takes_value(true)
            .required(false),
        Arg::with_name("PORT")
            .short('p')
            .long("port")
            .help("The port of the Wörterbuch server. When omitted, the value of the env var WORTERBUCH_PORT will be used. If that is not set, 4242 will be used.")
            .takes_value(true)
            .required(false),
        Arg::with_name("JSON")
            .short('j')
            .long("json")
            .help("Output data in JSON format instead of '[key]=[value]' pairs.")
            .takes_value(false)
            .required(false),
    ];

    args
}
