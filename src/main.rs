#![windows_subsystem = "windows"]

pub mod app;
pub mod model;

mod exporter;
mod fusion;
mod inspector;
mod loader;
mod pipeline;
mod processor;
mod service;
mod visualization;
fn main() -> Result<(), slint::PlatformError> {
    app::run()
}
