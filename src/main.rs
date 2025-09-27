// src/main.rs

// This file is no longer the primary entry point.
// The main logic has been moved to `src/bin/bot.rs` and `src/bin/server.rs`.
// This file is kept to avoid breaking builds that might still reference it,
// but it should ideally be removed from the project structure.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    Ok(())
}