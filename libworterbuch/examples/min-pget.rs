use libworterbuch::client::{config::Config, tcp};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let config = Config::new()?;

    let mut con = tcp::connect(&config.proto, &config.host_addr, config.port).await?;

    if let Some(values) = con.pget_values("hello/#").await? {
        for value in values {
            println!("{value}");
        }
    } else {
        eprintln!("[no value]");
    }

    Ok(())
}
