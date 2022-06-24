use anyhow::Result;
use clap::{App, Arg, ArgMatches};
use libworterbuch::{
    codec::{Err, KeyValuePair, PState, State},
    config::Config,
};

pub fn print_pstate(msg: &PState, json: bool) {
    if json {
        match serde_json::to_string(msg) {
            Ok(json) => println!("{json}"),
            Err(e) => {
                eprintln!("Error converting message to json: {e}");
            }
        }
    } else {
        for KeyValuePair { key, value } in &msg.key_value_pairs {
            println!("{key}={value}");
        }
    }
}

pub fn print_state(msg: &State, json: bool) {
    if json {
        match serde_json::to_string(msg) {
            Ok(json) => println!("{json}"),
            Err(e) => {
                eprintln!("Error converting message to json: {e}");
            }
        }
    } else {
        if let Some(KeyValuePair { key, value }) = &msg.key_value {
            println!("{}={}", key, value);
        } else {
            eprintln!("No result.");
        }
    }
}

pub fn print_err(msg: &Err, json: bool) {
    if json {
        match serde_json::to_string(msg) {
            Ok(json) => println!("{json}"),
            Err(e) => {
                eprintln!("Error converting message to json: {e}");
            }
        }
    } else {
        eprintln!("server error {}: {}", msg.error_code, msg.metadata);
    }
}

pub fn app<'help>(
    name: &'help str,
    about: &'help str,
    include_json: bool,
    args: Vec<Arg<'help>>,
) -> Result<(ArgMatches, String, String, u16, bool)> {
    let config = Config::new()?;

    let mut app = App::new(name)
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(about);
    for arg in default_args(include_json).iter().chain(args.iter()) {
        app = app.arg(arg)
    }

    let matches = app.get_matches();

    #[cfg(feature = "web")]
    let default_proto = config.proto;

    #[cfg(not(feature = "web"))]
    let default_proto = "".to_owned();

    let default_host_addr = config.host_addr;

    #[cfg(feature = "tcp")]
    let default_port = config.tcp_port;
    #[cfg(feature = "ws")]
    let default_port = config.web_port;
    #[cfg(feature = "graphql")]
    let default_port = config.graphql_port;

    let proto = default_proto;
    let host_addr = matches
        .get_one::<String>("ADDR")
        .map(ToOwned::to_owned)
        .unwrap_or(default_host_addr);
    let port = matches
        .get_one::<u16>("PORT")
        .map(ToOwned::to_owned)
        .unwrap_or(default_port);

    let json = if include_json {matches.is_present("JSON")} else {false};

    Ok((matches, proto, host_addr, port, json))
}

fn default_args<'help>(include_json: bool) -> Vec<Arg<'help>> {
    let mut args = vec![
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
        
    ];

    if include_json {
        args.push(
            Arg::with_name("JSON")
                .short('j')
                .long("json")
                .help("Output data in JSON format instead of '[key]=[value]' pairs.")
                .takes_value(false)
                .required(false)
        );
    }

    args
}
