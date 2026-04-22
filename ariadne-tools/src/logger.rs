use std::env;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::SubscriberBuilder;
use tracing_subscriber::fmt::format::{DefaultFields, Format};

pub fn setup(module: &str, log_level: &str) {
    if env::var_os("RUST_LOG").is_none() {
        let env = format!("{module}={log_level}");
        unsafe {
            env::set_var("RUST_LOG", env);
        }
    }
    let subscriber = get_subscriber();
    subscriber.init();
}

pub fn get_subscriber() -> SubscriberBuilder<DefaultFields, Format, EnvFilter> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env(), // .add_directive("opentelemetry=TRACE".parse().unwrap())
                                           // .add_directive("opentelemetry-otlp=TRACE".parse().unwrap())
                                           // .add_directive("opentelemetry_otlp=TRACE".parse().unwrap())
                                           // .add_directive("opentelemetry_sdk=TRACE".parse().unwrap()),
        )
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_thread_names(true)
}
