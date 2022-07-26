use libworterbuch::client::{config::Config, tcp};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let config = Config::new()?;

    let mut con = tcp::connect(&config.proto, &config.host_addr, config.port).await?;

    if let Some(value) = con.get_value("hello").await? {
        println!("{value}");
    } else {
        eprintln!("[no value]");
    }

    Ok(())
}
