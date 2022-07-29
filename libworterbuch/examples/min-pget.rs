use libworterbuch::{
    client::{config::Config, tcp},
    codec::KeyValuePair,
};
use std::process;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let config = Config::new()?;

    let on_disconnect = async move {
        eprintln!("Connection to server lost.");
        process::exit(1);
    };

    let mut con =
        tcp::connect(&config.proto, &config.host_addr, config.port, on_disconnect).await?;

    if let Some(values) = con.pget_values("hello/#").await? {
        for KeyValuePair { key, value } in values {
            println!("{key}={value}");
        }
    } else {
        eprintln!("[no value]");
    }

    Ok(())
}
