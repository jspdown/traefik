//! A `traefik:plugin/middleware` component for the Traefik plugin tests.
//!
//! The `mode` configuration option selects the behavior under test. An
//! `X-Plugin-Mode` request header selects it for one request.

use exports::wasi::http::handler::Guest;
use wasi::cli::stdout;
use wasi::clocks::{monotonic_clock, system_clock};
use wasi::config::store;
use wasi::http::handler;
use wasi::http::types::{ErrorCode, Fields, Request, Response};
use wasi::random::random;

wit_bindgen::generate!({
    path: "../../wit",
    world: "traefik:plugin/middleware",
    generate_all,
});

struct Plugin;

export!(Plugin);

impl Guest for Plugin {
    async fn handle(request: Request) -> Result<Response, ErrorCode> {
        let mode = request
            .get_headers()
            .get("x-plugin-mode")
            .into_iter()
            .next()
            .and_then(|value| String::from_utf8(value).ok())
            .or_else(|| config("mode"));

        match mode.as_deref() {
            // Traps, which poisons the instance.
            Some("panic") => panic!("panic mode"),
            // Answers with the content of the `file` option and the values of
            // the environment variables in the `envs` option.
            Some("respond") => respond(),
            // Forwards the request as is.
            Some("next") => handler::handle(request).await,
            // Forwards a copy of the request with an extra header, and adds a
            // header to the response.
            Some("headers") => headers(request).await,
            // Answers with the request body.
            Some("echo") => echo(request).await,
            // Forwards the request as is, and upper-cases the response body.
            Some("uppercase") => uppercase(request).await,
            // Answers with what the WASI 0.3 clocks, random and stdout
            // interfaces returned.
            Some("wasi") => wasi().await,
            mode => Err(internal_error(format!("unknown mode {mode:?}"))),
        }
    }
}

fn respond() -> Result<Response, ErrorCode> {
    let mut body = Vec::new();

    if let Some(file) = config("file") {
        body = std::fs::read(&file).map_err(|err| internal_error(format!("reading {file}: {err}")))?;
    }

    if let Some(envs) = config("envs") {
        for name in parse_string_list(&envs) {
            body.extend_from_slice(std::env::var(name).unwrap_or_default().as_bytes());
            body.push(b'\n');
        }
    }

    new_response(200, Fields::new(), body)
}

async fn headers(request: Request) -> Result<Response, ErrorCode> {
    let value = config("value").unwrap_or_default();

    let headers = request.get_headers().clone();
    headers
        .append("x-plugin-request", value.as_bytes())
        .map_err(|err| internal_error(format!("appending request header: {err:?}")))?;

    let method = request.get_method();
    let path_with_query = request.get_path_with_query();
    let scheme = request.get_scheme();
    let authority = request.get_authority();

    let (_, res) = wit_future::new(|| Ok(()));
    let (body, trailers) = Request::consume_body(request, res);

    let (forwarded, _) = Request::new(headers, Some(body), trailers, None);
    forwarded
        .set_method(&method)
        .and_then(|_| forwarded.set_path_with_query(path_with_query.as_deref()))
        .and_then(|_| forwarded.set_scheme(scheme.as_ref()))
        .and_then(|_| forwarded.set_authority(authority.as_deref()))
        .map_err(|_| internal_error("copying the request".to_string()))?;

    let response = handler::handle(forwarded).await?;

    let headers = response.get_headers().clone();
    headers
        .append("x-plugin-response", value.as_bytes())
        .map_err(|err| internal_error(format!("appending response header: {err:?}")))?;

    let status = response.get_status_code();

    let (_, res) = wit_future::new(|| Ok(()));
    let (body, trailers) = Response::consume_body(response, res);

    let (wrapped, _) = Response::new(headers, Some(body), trailers);
    wrapped
        .set_status_code(status)
        .map_err(|_| internal_error("copying the response status".to_string()))?;

    Ok(wrapped)
}

async fn echo(request: Request) -> Result<Response, ErrorCode> {
    let (_, res) = wit_future::new(|| Ok(()));
    let (body, trailers) = Request::consume_body(request, res);

    let body = body.collect().await;
    trailers.await?;

    new_response(200, Fields::new(), body)
}

async fn uppercase(request: Request) -> Result<Response, ErrorCode> {
    let response = handler::handle(request).await?;

    let headers = response.get_headers().clone();
    let _ = headers.delete("content-length");
    let status = response.get_status_code();

    let (_, res) = wit_future::new(|| Ok(()));
    let (body, trailers) = Response::consume_body(response, res);

    let body = body.collect().await.to_ascii_uppercase();
    trailers.await?;

    new_response(status, headers, body)
}

async fn wasi() -> Result<Response, ErrorCode> {
    let start = monotonic_clock::now();
    monotonic_clock::wait_for(1_000_000).await;
    let waited = monotonic_clock::now() - start >= 1_000_000;

    let now = system_clock::now();
    let random = random::get_random_bytes(16);

    let (mut tx, rx) = wit_stream::new();
    let written = stdout::write_via_stream(rx);
    tx.write_all(b"hello from wasi:cli/stdout@0.3\n".to_vec()).await;
    drop(tx);
    let stdout_ok = written.await.is_ok();

    let body = format!(
        "waited={waited} now={} random={} stdout={stdout_ok}",
        now.seconds > 0,
        random.len()
    );

    new_response(200, Fields::new(), body.into_bytes())
}

/// Builds a response whose body a spawned task writes after the handler
/// returns, as a streaming guest does.
fn new_response(status: u16, headers: Fields, body: Vec<u8>) -> Result<Response, ErrorCode> {
    let (mut contents_tx, contents_rx) = wit_stream::new();
    let (_, trailers) = wit_future::new(|| Ok(None));

    let (response, _) = Response::new(headers, Some(contents_rx), trailers);
    response
        .set_status_code(status)
        .map_err(|_| internal_error(format!("invalid status {status}")))?;

    wit_bindgen::spawn_local(async move {
        contents_tx.write_all(body).await;
    });

    Ok(response)
}

fn config(key: &str) -> Option<String> {
    store::get(key).ok().flatten()
}

/// Parses a JSON array of strings. It handles the values the tests use, not
/// the full JSON grammar.
fn parse_string_list(value: &str) -> impl Iterator<Item = &str> {
    value
        .trim()
        .trim_start_matches('[')
        .trim_end_matches(']')
        .split(',')
        .map(|item| item.trim().trim_matches('"'))
        .filter(|item| !item.is_empty())
}

fn internal_error(message: String) -> ErrorCode {
    ErrorCode::InternalError(Some(message))
}
