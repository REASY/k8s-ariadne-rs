use std::env;
use tracing_subscriber::fmt::format::{DefaultFields, Format};
use tracing_subscriber::fmt::SubscriberBuilder;
use tracing_subscriber::EnvFilter;

pub fn setup(log_level: &str) {
    if env::var_os("RUST_LOG").is_none() {
        let env = format!("ariadne_app={log_level},tower_http=WARN,hyper=WARN");
        env::set_var("RUST_LOG", env);
    }
    let subscriber = get_subscriber();
    subscriber.init();
}

pub fn get_subscriber() -> SubscriberBuilder<DefaultFields, Format, EnvFilter> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_thread_names(true)
}
