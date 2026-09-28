use std::env;
use std::error::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const DEFAULT_ADDR: &str = "127.0.0.1:8080";
const BUFFER_SIZE: usize = 1024;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| DEFAULT_ADDR.to_string());
    println!("Starting echo server on {}...", addr);
    let listener = TcpListener::bind(&addr).await?;
    println!("Echo server listening on {}", addr);

    loop {
        let (mut stream, addr) = listener.accept().await?;
        println!("Accepted connection from {}", addr);
        tokio::spawn(async move {
            let mut buf = vec![0; BUFFER_SIZE];

            loop{
                match stream.read(&mut buf).await {
                    Ok(0) => {
                        println!("Connection closed by client: {}", addr);
                        break;
                    }
                    Ok(n) => {
                        println!("Received {} bytes from {}: {:?}", n, addr, &buf[..n]);
                        if let Err(e) = stream.write_all(&buf[..n]).await {
                            eprintln!("Failed to send response to {}: {}", addr, e);
                            break;
                        }
                    }
                    Err(e) => {
                        eprintln!("Failed to read from {}: {}", addr, e);
                        break;
                    }
                }
            }
        });
    }
    // Ok(())
}
