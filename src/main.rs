#![windows_subsystem = "windows"]

pub mod app;
pub mod model;

mod exporter;
mod inspector;
mod loader;
mod pipeline;
mod processor;
mod service;
fn main() -> Result<(), slint::PlatformError> {
    app::run()
}
