use libworterbuch::client::{config::Config, tcp};
use std::process;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let config = Config::new()?;

    let on_disconnect = async move {
        eprintln!("Connection to server lost.");
        process::exit(1);
    };

    let mut con =
        tcp::connect(&config.proto, &config.host_addr, config.port, on_disconnect).await?;

    if let Some(value) = con.get_value("hello").await? {
        println!("{value}");
    } else {
        eprintln!("[no value]");
    }

    Ok(())
}
