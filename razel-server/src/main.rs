#[cfg(all(any(feature = "server", feature = "ssr"), not(feature = "hydrate")))]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_new(
                std::env::var("RUST_LOG")
                    .unwrap_or("info,cranelift=info,wasmtime=info".to_string()),
            )
            .expect("failed to parse tracing directives"),
        )
        .with_writer(std::io::stderr)
        .init();

    // exit on panic in any thread
    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        default_panic(info);
        std::process::exit(1);
    }));

    #[cfg(feature = "server")]
    server_main().await?;
    #[cfg(not(feature = "server"))]
    webui_main().await;
    Ok(())
}

#[cfg(all(feature = "server", not(feature = "hydrate")))]
async fn server_main() -> anyhow::Result<()> {
    use clap::Parser;
    use razel_server::Server;
    use razel_server::config::Config;
    use razel_server::webui_types::Stats;
    use std::path::PathBuf;

    #[derive(Parser)]
    struct Cli {
        /// config file to read
        #[clap(short, long, env = "RAZEL_SERVER_CONFIG")]
        config: PathBuf,
        /// node name from config file
        #[clap(short, long, env = "RAZEL_SERVER_NAME")]
        name: String,
    }

    let cli = if std::env::var("LEPTOS_RELOAD_PORT").is_ok() {
        // to simplify running "cargo leptos serve"
        Cli {
            config: PathBuf::from("razel-server/examples/localhost.toml"),
            name: "localhost".into(),
        }
    } else {
        Cli::parse()
    };
    let config = Config::read(&cli.config)?;
    let (stats_tx, stats_rx) = tokio::sync::watch::channel(Stats::default());
    tokio::spawn(webui_main(stats_rx));
    Server::new(config, cli.name, stats_tx)?.run().await?;
    Ok(())
}

#[cfg(all(feature = "ssr", not(feature = "hydrate")))]
async fn webui_main(stats_rx: tokio::sync::watch::Receiver<razel_server::webui_types::Stats>) {
    use axum::Router;
    use leptos::prelude::*;
    use leptos_axum::{LeptosRoutes, generate_route_list};
    use razel_server::webui::app::*;

    let conf = get_configuration(None).unwrap();
    let addr = conf.leptos_options.site_addr;
    let leptos_options = conf.leptos_options;
    // Generate the list of routes in your Leptos App
    let routes = generate_route_list(App);

    let app = Router::new()
        .leptos_routes(&leptos_options, routes, {
            let leptos_options = leptos_options.clone();
            move || shell(leptos_options.clone())
        })
        .fallback(leptos_axum::file_and_error_handler(shell))
        .layer(axum::Extension(stats_rx))
        .with_state(leptos_options);

    // run our app with hyper
    // `axum::Server` is a re-export of `hyper::Server`
    tracing::info!("webui listening on http://{}", &addr);
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app.into_make_service())
        .await
        .unwrap();
}

#[cfg(feature = "hydrate")]
fn main() {
    unreachable!(
        "just for: cargo check --target=wasm32-unknown-unknown --no-default-features -F hydrate"
    )
}
