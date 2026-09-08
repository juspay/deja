use proc_macro::TokenStream;
use syn::{parse_macro_input, ItemFn, ItemTrait};

mod boundary;
mod instrument;
mod recordable;

/// Attribute macro that makes a trait recordable by generating a delegation macro.
///
/// # Usage
///
/// Apply to a trait definition (before `#[async_trait]`):
///
/// ```ignore
/// #[deja::recordable]
/// #[async_trait::async_trait]
/// pub trait AddressInterface {
///     async fn find_address_by_address_id(&self, id: &str) -> Result<Address>;
///     async fn update_address(&self, id: String, update: AddressUpdate) -> Result<Address>;
/// }
/// ```
///
/// This generates a `delegate_address_interface!` macro that can be invoked:
///
/// ```ignore
/// delegate_address_interface!(DejaStore, inner, hook, "storage");
/// ```
///
/// Which expands to an impl block where every method:
/// 1. Captures the call site via `#[track_caller]` + `Location::caller()`
/// 2. Records the operation start (trait name, method name, args)
/// 3. Delegates to `self.inner.method(args).await`
/// 4. Records the result and duration
/// 5. Returns the result unchanged
#[proc_macro_attribute]
pub fn recordable(attr: TokenStream, item: TokenStream) -> TokenStream {
    let trait_def = parse_macro_input!(item as ItemTrait);
    // `#[deja_derive::recordable(local)]` — no #[macro_export], for same-crate use
    let attr = attr.to_string();
    let local = attr.contains("local");
    let opaque = attr.contains("opaque");
    recordable::generate(trait_def, local, opaque).into()
}

/// Attribute macro for semantic boundary recording around a function.
///
/// The macro owns event start/finish boilerplate. The annotated function stays
/// otherwise unchanged and supplies only extraction expressions:
///
/// ```ignore
/// #[deja::boundary(
///     boundary = "http_outgoing",
///     component = "external_services::http_client",
///     operation = "send_request",
///     correlation = request_id_from(&request),
///     args = request_args(&request),
///     result = response_result(__deja_result),
/// )]
/// async fn send_request(...) -> Result<Response, Error> { ... }
/// ```
///
/// `correlation` must evaluate to `Option<String>`. `args` must evaluate to
/// `serde_json::Value`. `result` receives `__deja_result` as `&Output` and must
/// return `(serde_json::Value, bool)`, where the bool marks errors.
///
/// # `on_miss` — a declared Substitute-miss value
///
/// By default a `Substitute` boundary whose replay lookup MISSES fail-stops: it
/// panics, the host's request guard contains the unwind, and the correlation is
/// scored as a stop. `on_miss = <expr>` replaces that continuation with a value
/// the DECLARATION SITE supplies:
///
/// ```ignore
/// #[deja::boundary(boundary = "imc", replay = Substitute, on_miss = None)]
/// async fn get_val<T>(&self, key: CacheKey) -> Option<T> { … }
/// ```
///
/// The expression is evaluated ONLY on a genuine lookup miss, has the
/// boundary's return type, and — like `result = …` naming `__deja_result` — may
/// name `__deja_miss`, a [`deja::SubstituteMiss`] carrying the boundary,
/// component, method and args image that found no recorded answer, so a host
/// error enum can take it with `#[from]` and stay attributable. Deja never
/// constructs the host's error: it cannot name an `E` a `replay_ok` site never
/// declares. The site can, which is the whole inversion.
///
/// This does NOT hide the miss. The blocking NovelCall divergence is emitted by
/// the lookup before `on_miss` is reached; only the continuation changes, so the
/// subtree that needed the value diverges and the graph tier localises it
/// instead of the request dying with no response at all.
///
/// Legality is the declaration's burden. `None` from a cache read is honest —
/// it means "not in cache", which is TRUE on replay, and the caller's fallback
/// is separately instrumented. A fabricated egress response is not: it claims a
/// third party answered when none did, and launders the divergence into a false
/// pass. Egress (http/grpc) keeps the fail-stop default. `on_miss` on a site
/// that resolves to `replay = Execute` is rejected at build: that branch never
/// reaches the Substitute-miss arm, so the declaration would be dead.
#[proc_macro_attribute]
pub fn boundary(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr as boundary::BoundaryArgs);
    let func = parse_macro_input!(item as ItemFn);
    boundary::generate(args, func).into()
}

/// Attribute macro for generic tracing-like semantic function recording.
///
/// Defaults:
/// - `boundary = "function"`
/// - `component = module_path!()`
/// - `operation = <function name>`
/// - args captured per-argument via `deja::capture!` (structured serde when the
///   type is `Serialize`, tagged `{"debug": …}` fallback when only `Debug`,
///   opaque type-name marker otherwise); result captured with `Debug` unless a
///   `replay_codec` is declared
///
/// Supported options include `boundary`, `component`, `operation`, `skip(...)`,
/// `skip_all`, `fields(...)`, `correlation = ...`, `args = ...`,
/// `result = ...`, `ret`, `err`, and `future = "boxed"`.
#[proc_macro_attribute]
pub fn instrument(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr as instrument::InstrumentArgs);
    let func = parse_macro_input!(item as ItemFn);
    instrument::generate(args, func).into()
}

#[proc_macro_attribute]
pub fn redis(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr as instrument::InstrumentArgs);
    let func = parse_macro_input!(item as ItemFn);
    // Kit preset: defaults boundary = "redis", kind = "redis", and
    // replay = Execute — redis reads + idempotent writes re-run against the
    // per-correlation seeded+isolated store (R1). A site declares only what is
    // genuinely site-specific (operation override, state_read/write, codec/args).
    // Ops that are UNSAFE to re-execute (accumulative RMW, destructive,
    // conditional, stream consumer-group, EVAL) declare `replay = Substitute`
    // at the site and serve the recorded result instead of double-applying.
    instrument::generate_with_preset(args, func, Some("redis"), instrument::Preset::Redis).into()
}

#[proc_macro_attribute]
pub fn http(attr: TokenStream, item: TokenStream) -> TokenStream {
    instrument::generate_http(attr, item).into()
}

#[proc_macro_attribute]
pub fn time(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr as instrument::InstrumentArgs);
    let func = parse_macro_input!(item as ItemFn);
    // Preset (#28): replay_strategy = Substitute, kind = "time" (clock is reconstructed, never re-run).
    instrument::generate_with_preset(args, func, Some("time"), instrument::Preset::Time).into()
}

#[proc_macro_attribute]
pub fn id(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr as instrument::InstrumentArgs);
    let func = parse_macro_input!(item as ItemFn);
    // Preset (#28): replay_strategy = Substitute, kind = "id" (entropy is reconstructed, never re-run).
    instrument::generate_with_preset(args, func, Some("id"), instrument::Preset::Id).into()
}
