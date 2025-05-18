// main.rs
use data_validation_service::run_server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Set up logging
    env_logger::init();
    
    // Run the server
    run_server("0.0.0.0:50052").await?;
    
    Ok(())
}
