use slint_build::CompileError;

fn main() -> Result<(), CompileError> {
    let config = slint_build::CompilerConfiguration::new().with_style("fluent-light".into());
    slint_build::compile_with_config("ui/main.slint", config)
}
