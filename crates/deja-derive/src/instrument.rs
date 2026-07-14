use std::collections::BTreeSet;

use proc_macro2::TokenStream;
use quote::quote;
use syn::{
    parenthesized,
    parse::{Parse, ParseStream},
    parse_quote, Expr, FnArg, Ident, ItemFn, LitStr, Pat, Path, Result, Token,
};

pub fn generate(args: InstrumentArgs, func: ItemFn) -> TokenStream {
    generate_with_boundary(args, func, None)
}

pub fn generate_with_boundary(
    args: InstrumentArgs,
    func: ItemFn,
    default_boundary: Option<&'static str>,
) -> TokenStream {
    generate_with_preset(args, func, default_boundary, Preset::None)
}

/// Declarative-boundary PRESET a macro entry point supplies. After the #28 knob
/// collapse a preset's only routing role is the per-site `replay_strategy`
/// default; the entropy/egress presets default to `Substitute` (never re-run) and
/// carry a descriptive `kind` label for the dashboard. An explicit
/// `replay = ...` argument still overrides the preset default.
/// `Preset::None` declares nothing — the site stays undeclared (default
/// `Substitute`, no label) and falls back to the runtime heuristics.
#[derive(Clone, Copy)]
pub enum Preset {
    /// No declared defaults (`#[deja::instrument]`, ...).
    None,
    /// `deja::redis` ⇒ `Execute`, `kind = "redis"`. The default for redis reads +
    /// idempotent writes (GET/SET/HSET/DEL/EXPIRE/…): re-run the real command
    /// against the per-correlation seeded+isolated store (R1). Safe because the
    /// store is rebuilt per test case (no cross-case collision / RMW double-apply).
    /// Ops that are NOT safe to re-execute — accumulative RMW (INCR/SADD/RPUSH/
    /// XADD), destructive (LPOP/XDEL/XTRIM), conditional (SETNX/HSETNX/MSETNX —
    /// depend on pre-state), stream consumer-group mutations, and EVAL — declare
    /// `replay = Substitute` at the site: re-running those would double-apply /
    /// consume / mis-branch without a recorded pre-image, so they serve the
    /// recorded result instead. (The invariant: accumulative-RMW + egress may
    /// never be Execute.)
    Redis,
    /// `deja::id` ⇒ `Substitute`, `kind = "id"` (entropy is reconstructed, never re-run).
    Id,
    /// `deja::time` ⇒ `Substitute`, `kind = "time"`.
    Time,
    /// `deja::http(outgoing)` ⇒ `Substitute`, `kind = "http"` (egress never re-issued).
    HttpOutgoing,
    /// `deja::http(incoming)` ⇒ declares NOTHING. Ingress is the replay DRIVER
    /// (correlation seed), not a reconstructed effect. We still record the
    /// `http_incoming` event exactly as before (it drives replay), but declare
    /// nothing — the boundary stays undeclared and the heuristic fallback applies.
    HttpIncoming,
}

pub fn generate_with_preset(
    mut args: InstrumentArgs,
    func: ItemFn,
    default_boundary: Option<&'static str>,
    preset: Preset,
) -> TokenStream {
    if args.boundary.is_none() {
        args.boundary = default_boundary.map(lit_str);
    }

    generate_inner(args, func, preset)
}

pub fn generate_http(attr: proc_macro::TokenStream, item: proc_macro::TokenStream) -> TokenStream {
    let (boundary, args) = parse_http_args(attr);
    // `deja::http(incoming)` is the replay DRIVER (correlation seed): it still
    // records the `http_incoming` event exactly as before, but declares NO channel
    // (ingress is outside the effect taxonomy now). Every other http boundary is
    // the Egress preset.
    let preset = if boundary == "http_incoming" {
        Preset::HttpIncoming
    } else {
        Preset::HttpOutgoing
    };
    match syn::parse::<ItemFn>(item) {
        Ok(func) => generate_with_preset(args, func, Some(boundary), preset),
        Err(error) => error.to_compile_error(),
    }
}

fn generate_inner(args: InstrumentArgs, mut func: ItemFn, preset: Preset) -> TokenStream {
    let sig = &func.sig;
    let vis = &func.vis;
    let block = &func.block;

    let boundary = args.boundary.unwrap_or_else(|| lit_str("function"));
    let boundary_str = boundary.value();
    let component = args
        .component
        .map_or_else(|| quote!(module_path!()), |value| quote!(#value));
    // The string form of `operation`, known at expansion time: the explicit
    // `operation = "..."` literal, or the function's own name. Used to derive a
    // stable syntactic hash and the rank-3 occurrence scope.
    let operation_str = args
        .operation
        .as_ref()
        .map_or_else(|| sig.ident.to_string(), |value| value.value());
    let operation = args.operation.map_or_else(
        || {
            let name = sig.ident.to_string();
            quote!(#name)
        },
        |value| quote!(#value),
    );

    // DECLARATIVE BOUNDARY MODEL (#28 — one knob): resolve the per-site
    // `replay` knob (preset default ⊕ explicit arg) into the `BoundarySpec`
    // constructor expression used at every emit site. When NOTHING is declared
    // this is exactly `BoundarySpec::new(#boundary, #component, #operation)` —
    // byte-identical tokens to before, so undeclared sites are unchanged. On a
    // bad `replay` variant this is a `compile_error!` token surfaced now.
    let declaration_args = DeclarationArgs {
        replay: args.replay.as_ref(),
        effect: args.effect.as_ref(),
        op: args.op.as_ref(),
        returns: args.returns.as_ref(),
    };
    let boundary_spec_expr =
        match build_boundary_spec_expr(&boundary, &component, &operation, declaration_args, preset)
        {
            Ok(expr) => expr,
            Err(err) => return err,
        };

    let state_read = args.state_read;
    let state_write = args.state_write;
    let state_touch = args.state_touch;
    let read_set = args.read_set;
    let write_set = args.write_set;
    let crossing_observation_expr = build_crossing_observation_expr(
        &boundary_spec_expr,
        state_read.as_ref(),
        state_write.as_ref(),
        state_touch.as_ref(),
        read_set.as_ref(),
        write_set.as_ref(),
    );

    let args_expr = args
        .args
        .unwrap_or_else(|| inferred_args_expr(sig, &args.skip, args.skip_all, &args.fields));
    let has_state_capture = state_read.is_some()
        || state_write.is_some()
        || state_touch.is_some()
        || read_set.is_some()
        || write_set.is_some();
    let eager_args_binding = if has_state_capture {
        quote! {
            // Evaluate args EAGERLY into an owned value before state-capture
            // expressions may move key collections, and before the run thunk moves
            // borrowed inputs. Keep the existing inactive-path args gate intact.
            let __deja_boundary_args = if ::deja::__private::capture_is_active() {
                #args_expr
            } else {
                ::serde_json::Value::Null
            };
        }
    } else {
        quote! {}
    };
    let args_thunk_expr = if has_state_capture {
        quote! { move || __deja_boundary_args }
    } else {
        quote! {
            {
                // Evaluate args EAGERLY into an owned value (ending any
                // borrow it holds, e.g. `&request`) BEFORE the run thunk
                // moves that value; then hand `dispatch` an owning thunk.
                // Gated so the inactive path never runs the args expr.
                let __deja_boundary_args = if ::deja::__private::capture_is_active() {
                    #args_expr
                } else {
                    ::serde_json::Value::Null
                };
                move || __deja_boundary_args
            }
        }
    };

    // --- CallsiteIdentity (rank-2 SpanPath + rank-3 SyntacticHash + rank-4
    //     LexicalPath) ------------------------------------------------------------
    //
    // A proc-macro attribute sees the function DEFINITION tokens, not the
    // invocation site, so it cannot hash the true call-site syntax. What it CAN
    // hash deterministically — and what stays stable across source line shifts
    // AND benign function-signature edits — is `boundary :: operation`.
    //
    // We DELIBERATELY do NOT fold the signature into the hash. Deja's purpose is
    // CROSS-VERSION regression (record on V1, replay on V2): a benign signature
    // edit (param rename/reorder, a type-alias change, a return-type tweak) must
    // NOT change a call-site's syntactic-hash identity, or that address would
    // miss on V2 and the call would silently demote to a weaker/positional rank —
    // a false regression. This matches the hand-written DB path, which already
    // hashes `boundary::component::operation` with no signature (deja/src/lib.rs).
    //
    // We compute the FNV-1a hash here at expansion time and emit it as a `u64`
    // literal (the rank-3 SyntacticHash address). The rank-4 lexical path is the
    // runtime `module_path!()`, and the rank-2 SpanPath is the runtime
    // span-path (`current_span_path()`, stamped below). Identity emission
    // is ADDITIVE: rank-5 (caller location) and rank-6 (positional sequence)
    // remain intact as fallbacks via `addresses_for`.
    let syntax_hash_input = format!("{}::{}", boundary_str, operation_str);
    let syntax_hash_value = syntactic_hash(&syntax_hash_input);
    // The occurrence scope key: per-method, matching the delegate (recordable)
    // path's `"{trait}::{method}"` granularity.
    let identity_scope_expr = quote! { format!("{}::{}", #component, #operation) };
    // Build the `CallsiteIdentity` ONCE per invocation. `occurrence` is the only
    // runtime field: it is allocated EXACTLY ONCE here via
    // `next_boundary_occurrence` and then reused for BOTH the replay lookup and
    // the recorded event, keeping record/replay occurrence numbering aligned.
    // The correlation id used for the occurrence bucket is the explicit
    // correlation (if any) falling back to the ambient one — the same value the
    // recorded event carries — so the renderer and hook bucket identically.
    let identity_build: TokenStream = quote! {
        let __deja_identity_scope: ::std::string::String = { #identity_scope_expr };
        let __deja_identity_correlation: ::std::option::Option<::std::string::String> =
            match &__deja_boundary_correlation_id {
                ::std::option::Option::Some(c) => ::std::option::Option::Some(c.clone()),
                ::std::option::Option::None => ::deja::__private::current_correlation_id(),
            };
        let __deja_identity = ::deja::__private::CallsiteIdentity {
            version: 1,
            source: ::deja::__private::CallsiteSource::SyntacticHash,
            id: ::std::option::Option::None,
            scope: ::std::option::Option::Some(__deja_identity_scope.clone()),
            occurrence: ::deja::__private::next_boundary_occurrence(
                __deja_identity_correlation.as_deref(),
                ::deja::__private::CallsiteSource::SyntacticHash,
                ::std::option::Option::Some(__deja_identity_scope.as_str()),
            ),
            caller_function: ::std::option::Option::Some(::std::module_path!().to_string()),
            lexical_path: ::std::option::Option::Some(::std::module_path!().to_string()),
            syntax_hash: ::std::option::Option::Some(#syntax_hash_value),
            span_path: ::deja::__private::current_span_path(),
            extras: ::std::default::Default::default(),
        };
    };

    // RESULT CODEC selection (#27 / G2). `codec = <Codec>` is the replay
    // reconstruction selector. With no selector, the site is Debug/record-only;
    // the bare built-in names map to the serde / Ok-only codegen, and any other
    // path is a custom `::deja::codec::ReplayCodec` impl.
    let capture_mode = args
        .codec
        .as_ref()
        .map(classify_codec)
        .unwrap_or(CaptureMode::Debug);

    // The lossless capture expr handed to the dispatch seam as `extract`. An
    // explicit `result =` always wins (escape hatch); otherwise the explicit
    // `codec` selector supplies it. Record-only sites fall back to the cheap
    // (unrecoverable) Debug shape.
    let result_expr: Expr = args.result.clone().unwrap_or_else(|| match &capture_mode {
        CaptureMode::Custom(path) => {
            parse_quote!(<#path as ::deja::codec::ReplayCodec>::capture(__deja_result))
        }
        CaptureMode::Serde => parse_quote!(::deja::value::result_serialize(__deja_result)),
        CaptureMode::ResultOk => parse_quote!(::deja::value::result_serialize_ok(__deja_result)),
        CaptureMode::Debug => {
            parse_quote!(::deja::value::result_debug(__deja_result))
        }
    });
    let correlation_expr = args
        .correlation
        .unwrap_or_else(|| parse_quote!(None::<String>));

    // The function's return type, used to deserialize a replayed result.
    let ret_ty: TokenStream = match &sig.output {
        syn::ReturnType::Default => quote!(()),
        syn::ReturnType::Type(_, ty) => quote!(#ty),
    };

    // The type the reconstruct closure targets — i.e. the `T` that
    // `dispatch`/`dispatch_async` resolves to. For sync and `async fn` bodies
    // that is the return type itself. For a `future = "boxed"` body the seam
    // resolves the FUTURE'S OUTPUT (the macro re-wraps it in `Box::pin`), so the
    // reconstruct target is that inner output, not the un-deserializable
    // `Pin<Box<dyn Future>>`.
    let recon_ty: TokenStream = if matches!(args.future, Some(FutureMode::Boxed)) {
        boxed_future_output_ty(&sig.output).unwrap_or_else(|| ret_ty.clone())
    } else {
        ret_ty.clone()
    };

    if !func
        .attrs
        .iter()
        .any(|attr| attr.path().is_ident("track_caller"))
    {
        func.attrs.push(parse_quote!(#[track_caller]));
    }
    let attrs = &func.attrs;

    // The reconstruct closure handed to `dispatch`/`dispatch_async`. It is the
    // type-erased deserializer (design §3): on a lookup HIT, `dispatch` calls it
    // to turn the recorded JSON back into the return type. The closure returns
    // `Reconstructed::Value` for a real substituted value and `Failed` for
    // malformed or incompatible capture data.
    //
    //  - Custom(codec): `<codec as ReplayCodec>::reconstruct` — non-serde results
    //    (HTTP response, DB envelope) plug in here; `None` is a reconstruction failure.
    //  - ResultOk: Result Ok-only — deserialize the recorded value into the Ok type
    //    `R` (first generic arg) and return `Value(Ok(R))`; error sentinels and
    //    malformed capture return `Failed`.
    //  - Serde: direct — deserialize into the whole return type.
    //  - Debug (record-only): `Failed` — `dispatch` never reaches it (the lookup
    //    seam returns `None` for a recording hook), AND the `DeserializeOwned`
    //    capability is confined to this closure so record-only return types need no
    //    serde bound.
    let reconstruct_closure: TokenStream = match &capture_mode {
        CaptureMode::Custom(path) => quote! {
            |__deja_recorded: ::serde_json::Value| -> ::deja::__private::Reconstructed<#recon_ty> {
                match <#path as ::deja::codec::ReplayCodec>::reconstruct(__deja_recorded) {
                    ::std::option::Option::Some(__deja_replayed) =>
                        ::deja::__private::Reconstructed::Value(__deja_replayed),
                    ::std::option::Option::None => ::deja::__private::Reconstructed::Failed,
                }
            }
        },
        CaptureMode::ResultOk => {
            // The Ok type to deserialize into: the first generic arg of the
            // reconstruct target (`CustomResult<R, E>` → `R`). For boxed bodies
            // that target is the future's output, so this reaches the right Result.
            let ok_ty = match first_generic_arg_of_output(&sig.output, args.future) {
                Some(ty) => ty,
                None => {
                    return syn::Error::new_spanned(
                        &sig.ident,
                        "`codec = ResultOkCodec` requires a Result-like return type with a generic Ok argument (e.g. CustomResult<R, E>)",
                    )
                    .to_compile_error();
                }
            };
            quote! {
                |__deja_recorded: ::serde_json::Value| -> ::deja::__private::Reconstructed<#recon_ty> {
                    if __deja_recorded
                        .as_object()
                        .is_some_and(|__deja_map| __deja_map.contains_key("deja_err"))
                    {
                        return ::deja::__private::Reconstructed::Failed;
                    }
                    match ::serde_json::from_value::<#ok_ty>(__deja_recorded) {
                        ::std::result::Result::Ok(__deja_replayed) =>
                            ::deja::__private::Reconstructed::Value(::std::result::Result::Ok(__deja_replayed)),
                        ::std::result::Result::Err(_) => ::deja::__private::Reconstructed::Failed,
                    }
                }
            }
        }
        CaptureMode::Serde => quote! {
            |__deja_recorded: ::serde_json::Value| -> ::deja::__private::Reconstructed<#recon_ty> {
                match ::serde_json::from_value::<#recon_ty>(__deja_recorded) {
                    ::std::result::Result::Ok(__deja_replayed) =>
                        ::deja::__private::Reconstructed::Value(__deja_replayed),
                    ::std::result::Result::Err(_) => ::deja::__private::Reconstructed::Failed,
                }
            }
        },
        CaptureMode::Debug => quote! {
            |_: ::serde_json::Value| -> ::deja::__private::Reconstructed<#recon_ty> {
                ::deja::__private::Reconstructed::Failed
            }
        },
    };

    if sig.asyncness.is_some() {
        if args.future.is_some() {
            return syn::Error::new_spanned(
                &sig.ident,
                "`future = \"boxed\"` is only valid on non-async functions that return a boxed future",
            )
            .to_compile_error();
        }

        // ONE shape: build identity (occurrence allocated once), then call the
        // single `dispatch_async` seam. The macro names NO replay-only operation:
        // it hands `dispatch_async` the args thunk, the block as a run thunk, a
        // typed reconstruct closure, and the lossless result extractor. All of
        // run/skip/shadow/record control flow lives inside the seam.
        quote! {
            #(#attrs)*
            #vis #sig {
                let __deja_boundary_correlation_id: Option<String> = { #correlation_expr };
                #identity_build
                #eager_args_binding
                ::deja::__private::dispatch_async(
                    #crossing_observation_expr,
                    #args_thunk_expr,
                    move || async move #block,
                    #reconstruct_closure,
                    move |__deja_result| { #result_expr },
                ).await
            }
        }
    } else if matches!(args.future, Some(FutureMode::Boxed)) {
        // Boxed-future shape: the fn is sync but returns `Pin<Box<dyn Future>>`.
        // `dispatch_async` resolves the inner future and yields the inner output;
        // the macro wraps that in `Box::pin`. The run thunk evaluates the block
        // (which yields the inner future) and awaits it.
        quote! {
            #(#attrs)*
            #vis #sig {
                let __deja_boundary_correlation_id: Option<String> = { #correlation_expr };
                #identity_build
                #eager_args_binding
                ::std::boxed::Box::pin(::deja::__private::dispatch_async(
                    #crossing_observation_expr,
                    #args_thunk_expr,
                    move || async move { #block.await },
                    #reconstruct_closure,
                    move |__deja_result| { #result_expr },
                ))
            }
        }
    } else {
        // Sync shape: the single `dispatch` seam, block as a sync run thunk.
        quote! {
            #(#attrs)*
            #vis #sig {
                let __deja_boundary_correlation_id: Option<String> = { #correlation_expr };
                #identity_build
                #eager_args_binding
                ::deja::__private::dispatch(
                    #crossing_observation_expr,
                    #args_thunk_expr,
                    || #block,
                    #reconstruct_closure,
                    move |__deja_result| { #result_expr },
                )
            }
        }
    }
}

fn build_crossing_observation_expr(
    boundary_spec_expr: &TokenStream,
    state_read: Option<&Expr>,
    state_write: Option<&Expr>,
    state_touch: Option<&Expr>,
    read_set: Option<&Expr>,
    write_set: Option<&Expr>,
) -> TokenStream {
    let mut observation = quote! {
        ::deja::__private::CrossingObservation::with_correlation(
            #boundary_spec_expr,
            __deja_identity,
            ::std::panic::Location::caller(),
            __deja_boundary_correlation_id,
        )
    };

    if let Some(expr) = state_read {
        observation = quote! { #observation.state_read_to(#expr) };
    }
    if let Some(expr) = state_write {
        observation = quote! { #observation.state_write_to(#expr) };
    }
    if let Some(expr) = state_touch {
        observation = quote! { #observation.state_touch_to(#expr) };
    }
    if let Some(expr) = read_set {
        observation = quote! {
            #observation.with_read_set(
                (#expr)
                    .into_iter()
                    .map(::std::convert::Into::into)
                    .collect::<Vec<String>>()
            )
        };
    }
    if let Some(expr) = write_set {
        observation = quote! {
            #observation.with_write_set(
                (#expr)
                    .into_iter()
                    .map(::std::convert::Into::into)
                    .collect::<Vec<String>>()
            )
        };
    }

    observation
}

/// The declaration knobs a site passes to [`build_boundary_spec_expr`]: the
/// per-site `replay` routing knob plus the non-routing typed metadata
/// (`effect` / `op` / `returns`).
#[derive(Clone, Copy)]
struct DeclarationArgs<'a> {
    replay: Option<&'a Ident>,
    effect: Option<&'a Ident>,
    op: Option<&'a Ident>,
    returns: Option<&'a Ident>,
}

/// Build the `BoundarySpec` constructor expression for the per-site knob model
/// (#28). Combines the PRESET default (`deja::id`/`time`/`http` → `Substitute` +
/// a `kind` label) with an EXPLICIT `replay = Execute | Substitute` argument
/// (explicit wins), and emits either:
///   - `BoundarySpec::new(b, c, o)` when NOTHING is declared (byte-identical to
///     the pre-#28 tokens — undeclared sites unchanged), or
///   - `BoundarySpec::with_semantics(b, c, o, BoundarySemantics { replay_strategy,
///     kind })` when a knob and/or label is declared.
///
/// Returns `Err(compile_error_tokens)` on a bad `replay` variant.
fn build_boundary_spec_expr(
    boundary: &LitStr,
    component: &TokenStream,
    operation: &TokenStream,
    declaration: DeclarationArgs<'_>,
    preset: Preset,
) -> std::result::Result<TokenStream, TokenStream> {
    let DeclarationArgs {
        replay,
        effect,
        op,
        returns,
    } = declaration;
    // Preset `kind` label (descriptive only, NOT routing). `HttpIncoming` /
    // `None` declare nothing.
    let preset_kind: Option<&'static str> = match preset {
        Preset::None | Preset::HttpIncoming => None,
        Preset::Redis => Some("redis"),
        Preset::Id => Some("id"),
        Preset::Time => Some("time"),
        Preset::HttpOutgoing => Some("http"),
    };
    let preset_effect: Option<TokenStream> = match preset {
        Preset::Redis => Some(quote!(::deja::__private::EffectKind::Redis)),
        Preset::Id => Some(quote!(::deja::__private::EffectKind::Entropy)),
        Preset::Time => Some(quote!(::deja::__private::EffectKind::Time)),
        Preset::HttpOutgoing => Some(quote!(::deja::__private::EffectKind::Http)),
        Preset::None | Preset::HttpIncoming => None,
    };
    let effect_tokens = match effect {
        Some(id) => Some(effect_kind_variant(id)?),
        None => preset_effect,
    };
    let op_tokens = match op {
        Some(id) => Some(operation_kind_variant(id)?),
        None => None,
    };
    let return_tokens = match returns {
        Some(id) => Some(return_semantics_variant(id)?),
        None => None,
    };
    let effect_field = effect_tokens
        .map(|tokens| quote!(::std::option::Option::Some(#tokens)))
        .unwrap_or_else(|| quote!(::std::option::Option::None));
    let op_field = op_tokens
        .map(|tokens| quote!(::std::option::Option::Some(#tokens)))
        .unwrap_or_else(|| quote!(::std::option::Option::None));
    let return_field = return_tokens
        .map(|tokens| quote!(::std::option::Option::Some(#tokens)))
        .unwrap_or_else(|| quote!(::std::option::Option::None));
    let has_declaration = effect.is_some()
        || op.is_some()
        || returns.is_some()
        || matches!(
            preset,
            Preset::Redis | Preset::Id | Preset::Time | Preset::HttpOutgoing
        );
    let declaration_field = if has_declaration {
        quote! {
            ::std::option::Option::Some(::deja::__private::BoundaryDeclaration {
                effect: #effect_field,
                op: #op_field,
                returns: #return_field,
                codec: ::std::option::Option::None,
                state_canon: ::std::option::Option::None,
                reply_canon: ::std::option::Option::None,
            })
        }
    } else {
        quote!(::std::option::Option::None)
    };

    // The preset's DEFAULT replay routing: `deja::redis` re-executes reads +
    // idempotent writes against the seeded isolated store (Execute); everything
    // else defaults to Substitute (entropy/egress/clock are never re-run; redis
    // ops that are unsafe to re-execute declare `replay = Substitute` at the
    // site). An explicit `replay` arg always overrides this default.
    let preset_default_strategy = match preset {
        Preset::Redis => quote!(::deja::__private::ReplayStrategy::Execute),
        _ => quote!(::deja::__private::ReplayStrategy::Substitute),
    };
    let strategy_tokens = match replay {
        Some(id) => replay_strategy_variant(id)?,
        None => preset_default_strategy,
    };

    // A site is DECLARED iff it carries an explicit knob, typed declaration
    // metadata, OR a preset `kind` label.
    let declared = replay.is_some() || has_declaration || preset_kind.is_some();
    if !declared {
        // Nothing declared → emit the byte-identical legacy constructor.
        return Ok(quote! {
            ::deja::__private::BoundarySpec::new(#boundary, #component, #operation)
        });
    }

    // An explicitly declared site with no preset still gets a non-`None` `kind`
    // for dashboard grouping and tape forensics. Routing is policy-free: only
    // `replay_strategy` decides Execute vs Substitute. Use the boundary tag as
    // the descriptive label. (This branch only runs when `declared` is true.)
    let kind_field = match preset_kind {
        Some(k) => quote!(::std::option::Option::Some(#k.to_string())),
        None => quote!(::std::option::Option::Some((#boundary).to_string())),
    };

    Ok(quote! {
        ::deja::__private::BoundarySpec::with_semantics(
            #boundary,
            #component,
            #operation,
            ::deja::__private::BoundarySemantics {
                replay_strategy: #strategy_tokens,
                kind: #kind_field,
                declaration: #declaration_field,
            },
        )
    })
}

/// Validate + map a `replay = <Ident>` to `ReplayStrategy::<Variant>` tokens.
fn replay_strategy_variant(id: &Ident) -> std::result::Result<TokenStream, TokenStream> {
    match id.to_string().as_str() {
        "Execute" | "Substitute" => Ok(quote!(::deja::__private::ReplayStrategy::#id)),
        _ => Err(
            syn::Error::new_spanned(id, "unknown replay; expected Execute | Substitute")
                .to_compile_error(),
        ),
    }
}

fn effect_kind_variant(id: &Ident) -> std::result::Result<TokenStream, TokenStream> {
    match id.to_string().as_str() {
        "Db" | "Redis" | "Http" | "Entropy" | "Time" | "Function" => {
            Ok(quote!(::deja::__private::EffectKind::#id))
        }
        _ => Err(syn::Error::new_spanned(
            id,
            "unknown effect; expected Db | Redis | Http | Entropy | Time | Function",
        )
        .to_compile_error()),
    }
}

fn operation_kind_variant(id: &Ident) -> std::result::Result<TokenStream, TokenStream> {
    match id.to_string().as_str() {
        "Read" | "Write" | "Touch" | "Create" | "Update" | "Delete" | "Upsert"
        | "CompareAndSet" | "IdempotentDelete" | "ExternalCall" | "Entropy" | "Clock" => {
            Ok(quote!(::deja::__private::OperationKind::#id))
        }
        _ => Err(syn::Error::new_spanned(
            id,
            "unknown op; expected Read | Write | Touch | Create | Update | Delete | Upsert | CompareAndSet | IdempotentDelete | ExternalCall | Entropy | Clock",
        )
        .to_compile_error()),
    }
}

fn return_semantics_variant(id: &Ident) -> std::result::Result<TokenStream, TokenStream> {
    match id.to_string().as_str() {
        "None" | "Unit" | "Value" | "Optional" | "Rows" | "Count" | "Bool" | "PreImage"
        | "PostImage" | "UpdateReturning" | "DeleteReturning" | "Raw" => {
            Ok(quote!(::deja::__private::ReturnSemantics::#id))
        }
        _ => Err(syn::Error::new_spanned(
            id,
            "unknown returns; expected None | Unit | Value | Optional | Rows | Count | Bool | PreImage | PostImage | UpdateReturning | DeleteReturning | Raw",
        )
        .to_compile_error()),
    }
}

/// How a boundary's result is captured/reconstructed from the explicit
/// `codec = <Codec>` selector (#27 / G2). Absence of a selector is
/// Debug/record-only.
enum CaptureMode {
    /// Whole-value serde (`codec = SerdeCodec`).
    Serde,
    /// `Result` Ok-arm only (`codec = ResultOkCodec`).
    ResultOk,
    /// A custom `::deja::codec::ReplayCodec` impl (`codec = some::Codec`).
    Custom(Path),
    /// Record-only — unrecoverable Debug capture, `|_| None` reconstruct.
    Debug,
}

/// Map a `codec = <Path>` value to a `CaptureMode`. The bare, unqualified,
/// generic-free names `SerdeCodec` / `ResultOkCodec` are treated as built-in
/// aliases (so the call site needs no generic arguments — the macro infers them
/// from the return type). Anything else — a qualified path, a generic
/// instantiation, or a different name — is a custom `ReplayCodec` impl.
fn classify_codec(path: &Path) -> CaptureMode {
    if path.leading_colon.is_none() && path.segments.len() == 1 {
        let seg = &path.segments[0];
        if seg.arguments.is_empty() {
            match seg.ident.to_string().as_str() {
                "SerdeCodec" => return CaptureMode::Serde,
                "ResultOkCodec" => return CaptureMode::ResultOk,
                _ => {}
            }
        }
    }
    CaptureMode::Custom(path.clone())
}

fn inferred_args_expr(
    sig: &syn::Signature,
    skipped: &[Ident],
    skip_all: bool,
    fields: &[FieldArg],
) -> Expr {
    let skipped: BTreeSet<String> = skipped.iter().map(ToString::to_string).collect();
    let mut inserts = Vec::new();

    if !skip_all {
        for input in &sig.inputs {
            let FnArg::Typed(pat_type) = input else {
                continue;
            };
            let Pat::Ident(pat_ident) = pat_type.pat.as_ref() else {
                continue;
            };
            let ident = &pat_ident.ident;
            let ident_string = ident.to_string();
            if ident_string == "_" || skipped.contains(&ident_string) {
                continue;
            }
            let key = ident_string;
            inserts.push(quote! {
                __deja_boundary_map.insert(
                    #key.to_string(),
                    ::deja::capture!(#ident),
                );
            });
        }
    }

    for field in fields {
        let key = field.name.value();
        let expr = &field.expr;
        inserts.push(quote! {
            __deja_boundary_map.insert(
                #key.to_string(),
                ::deja::capture!((#expr)),
            );
        });
    }

    parse_quote!({
        let mut __deja_boundary_map = ::serde_json::Map::new();
        #(#inserts)*
        ::serde_json::Value::Object(__deja_boundary_map)
    })
}

fn parse_http_args(attr: proc_macro::TokenStream) -> (&'static str, InstrumentArgs) {
    let attr_ts: TokenStream = attr.into();
    if attr_ts.is_empty() {
        return ("http_outgoing", InstrumentArgs::default());
    }

    let tokens: Vec<_> = attr_ts.clone().into_iter().collect();
    let Some(proc_macro2::TokenTree::Ident(first)) = tokens.first() else {
        let args = syn::parse2(attr_ts).unwrap_or_default();
        return ("http_outgoing", args);
    };

    let boundary = match first.to_string().as_str() {
        "incoming" => Some("http_incoming"),
        "outgoing" => Some("http_outgoing"),
        _ => None,
    };

    let Some(boundary) = boundary else {
        let args = syn::parse2(attr_ts).unwrap_or_default();
        return ("http_outgoing", args);
    };

    let rest = if matches!(
        tokens.get(1),
        Some(proc_macro2::TokenTree::Punct(punct)) if punct.as_char() == ','
    ) {
        tokens[2..].iter().cloned().collect()
    } else {
        TokenStream::new()
    };
    let args = syn::parse2(rest).unwrap_or_default();
    (boundary, args)
}

fn lit_str(value: &'static str) -> LitStr {
    LitStr::new(value, proc_macro2::Span::call_site())
}

/// FNV-1a hash of a string, computed at macro-expansion time and emitted as a
/// `u64` literal for `CallsiteIdentity::syntax_hash` (rank-3
/// `Address::SyntacticHash`).
///
/// MUST stay byte-for-byte identical to `deja_runtime::stable_callsite_hash`
/// (FNV-1a over the bytes, then a `0xff` terminator) so a hash computed here at
/// compile time matches one computed at runtime for the same input. FNV-1a is
/// chosen over `std::hash::DefaultHasher` because it is fully specified and
/// stable across rustc/syn versions — the input string (which includes the
/// function signature tokens) never changes its hash for a given logical
/// boundary, so record and replay agree regardless of source line shifts.
pub(crate) fn syntactic_hash(input: &str) -> u64 {
    const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = FNV_OFFSET_BASIS;
    for byte in input.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    // Terminator byte, matching `fnv1a_str` in deja-record.
    hash ^= u64::from(0xffu8);
    hash.wrapping_mul(FNV_PRIME)
}

/// Extract the first generic type argument of a function's return type — e.g.
/// `R` from `CustomResult<R, E>` or `StorageResult<R>`. Used by
/// `codec = ResultOkCodec` to find the `Ok` type to deserialize into,
/// without requiring the (possibly non-serde) error type.
fn first_generic_arg(output: &syn::ReturnType) -> Option<TokenStream> {
    let ty = match output {
        syn::ReturnType::Type(_, ty) => ty.as_ref(),
        syn::ReturnType::Default => return None,
    };
    first_generic_arg_of_type(ty)
}

/// `codec = ResultOkCodec`'s Ok-type extractor, aware of the
/// `future = "boxed"` shape. For a boxed body the reconstruct target is the
/// future's OUTPUT type (the macro re-wraps in `Box::pin`), so the Ok type is the
/// first generic of THAT, not of the `Pin<Box<dyn Future>>`. For all other bodies
/// it is the first generic of the return type.
fn first_generic_arg_of_output(
    output: &syn::ReturnType,
    future: Option<FutureMode>,
) -> Option<TokenStream> {
    if matches!(future, Some(FutureMode::Boxed)) {
        let inner = boxed_future_output_ty(output)?;
        let parsed: syn::Type = syn::parse2(inner).ok()?;
        first_generic_arg_of_type(&parsed)
    } else {
        first_generic_arg(output)
    }
}

/// Extract the first generic type argument of a type — e.g. `R` from
/// `CustomResult<R, E>` or `StorageResult<R>`.
fn first_generic_arg_of_type(ty: &syn::Type) -> Option<TokenStream> {
    if let syn::Type::Path(type_path) = ty {
        if let Some(segment) = type_path.path.segments.last() {
            if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                for arg in &args.args {
                    if let syn::GenericArgument::Type(inner) = arg {
                        return Some(quote!(#inner));
                    }
                }
            }
        }
    }
    None
}

/// Extract `X` from a `future = "boxed"` return type of the shape
/// `Pin<Box<dyn Future<Output = X> (+ Send)? (+ 'lt)?>>`. This is the type the
/// `dispatch_async` seam resolves to for a boxed body (the macro re-wraps it in
/// `Box::pin`), and hence the reconstruct closure's target type. Returns `None`
/// if the return type does not match that shape, in which case the caller falls
/// back to the whole return type (record-only boxed bodies never reach the
/// reconstruct path, so the fallback is harmless there).
fn boxed_future_output_ty(output: &syn::ReturnType) -> Option<TokenStream> {
    fn find_future_output(ty: &syn::Type) -> Option<TokenStream> {
        match ty {
            // `Pin<...>` / `Box<...>` — descend into the angle-bracketed arg.
            syn::Type::Path(type_path) => {
                let segment = type_path.path.segments.last()?;
                if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                    for arg in &args.args {
                        match arg {
                            syn::GenericArgument::Type(inner) => {
                                if let Some(found) = find_future_output(inner) {
                                    return Some(found);
                                }
                            }
                            // `Output = X` on a `dyn Future` bound.
                            syn::GenericArgument::AssocType(assoc) if assoc.ident == "Output" => {
                                let bound = &assoc.ty;
                                return Some(quote!(#bound));
                            }
                            _ => {}
                        }
                    }
                }
                None
            }
            // `dyn Future<Output = X> + ...` / `impl Future<Output = X>`.
            syn::Type::TraitObject(obj) => {
                for bound in &obj.bounds {
                    if let Some(found) = future_output_from_bound(bound) {
                        return Some(found);
                    }
                }
                None
            }
            syn::Type::ImplTrait(it) => {
                for bound in &it.bounds {
                    if let Some(found) = future_output_from_bound(bound) {
                        return Some(found);
                    }
                }
                None
            }
            _ => None,
        }
    }

    fn future_output_from_bound(bound: &syn::TypeParamBound) -> Option<TokenStream> {
        if let syn::TypeParamBound::Trait(trait_bound) = bound {
            let segment = trait_bound.path.segments.last()?;
            if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                for arg in &args.args {
                    if let syn::GenericArgument::AssocType(assoc) = arg {
                        if assoc.ident == "Output" {
                            let bound_ty = &assoc.ty;
                            return Some(quote!(#bound_ty));
                        }
                    }
                }
            }
        }
        None
    }

    let ty = match output {
        syn::ReturnType::Type(_, ty) => ty.as_ref(),
        syn::ReturnType::Default => return None,
    };
    find_future_output(ty)
}

#[derive(Default)]
pub struct InstrumentArgs {
    pub boundary: Option<LitStr>,
    pub component: Option<LitStr>,
    pub operation: Option<LitStr>,
    pub args: Option<Expr>,
    pub result: Option<Expr>,
    pub correlation: Option<Expr>,
    pub future: Option<FutureMode>,
    pub skip_all: bool,
    pub skip: Vec<Ident>,
    pub fields: Vec<FieldArg>,
    pub ret: bool,
    pub err: bool,
    /// RESULT CODEC (#27 / G2). The per-site result codec: `codec = <Path>`.
    /// With no selector, the site is Debug/record-only. The two bare built-in
    /// names `SerdeCodec` (whole-value serde) and `ResultOkCodec` (Ok-only
    /// serde) are recognized and expand to the proven serde codegen with the
    /// generics inferred from the return type. Any other path is treated as a
    /// custom `::deja::codec::ReplayCodec` impl whose `Value` must equal the
    /// return type.
    pub codec: Option<Path>,
    /// DECLARATIVE BOUNDARY MODEL (#28+). The per-site replay routing knob:
    /// `replay = Execute | Substitute` (bare enum-variant identifier; the macro
    /// maps it to a `deja::__private::ReplayStrategy` value — the wire field on
    /// events keeps the name `replay_strategy`). Additional typed metadata
    /// (`effect`, `op`, `returns`) is metadata-only for seed planning /
    /// reporting; it never overrides `replay`.
    pub replay: Option<Ident>,
    pub effect: Option<Ident>,
    pub op: Option<Ident>,
    pub returns: Option<Ident>,
    pub state_read: Option<Expr>,
    pub state_write: Option<Expr>,
    pub state_touch: Option<Expr>,
    pub read_set: Option<Expr>,
    pub write_set: Option<Expr>,
}

#[derive(Clone, Copy)]
pub enum FutureMode {
    Boxed,
}

pub struct FieldArg {
    name: LitStr,
    expr: Expr,
}

impl Parse for InstrumentArgs {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let mut args = Self::default();

        while !input.is_empty() {
            let key: Ident = input.parse()?;
            let key_string = key.to_string();

            match key_string.as_str() {
                "skip" => {
                    let content;
                    parenthesized!(content in input);
                    while !content.is_empty() {
                        args.skip.push(content.parse()?);
                        if content.peek(Token![,]) {
                            content.parse::<Token![,]>()?;
                        }
                    }
                }
                "fields" => {
                    let content;
                    parenthesized!(content in input);
                    while !content.is_empty() {
                        let field_name: Ident = content.parse()?;
                        content.parse::<Token![=]>()?;
                        let expr: Expr = content.parse()?;
                        args.fields.push(FieldArg {
                            name: LitStr::new(&field_name.to_string(), field_name.span()),
                            expr,
                        });
                        if content.peek(Token![,]) {
                            content.parse::<Token![,]>()?;
                        }
                    }
                }
                "skip_all" => args.skip_all = true,
                "ret" => args.ret = true,
                "err" => args.err = true,
                _ => {
                    if !input.peek(Token![=]) {
                        return Err(syn::Error::new(
                            key.span(),
                            "unsupported deja instrument argument",
                        ));
                    }
                    input.parse::<Token![=]>()?;
                    match key_string.as_str() {
                        "boundary" => args.boundary = Some(input.parse()?),
                        "component" => args.component = Some(input.parse()?),
                        "operation" => args.operation = Some(input.parse()?),
                        "args" => args.args = Some(input.parse()?),
                        "result" => args.result = Some(input.parse()?),
                        "codec" => args.codec = Some(input.parse()?),
                        "state_read" => args.state_read = Some(input.parse()?),
                        "state_write" => args.state_write = Some(input.parse()?),
                        "state_touch" => args.state_touch = Some(input.parse()?),
                        "read_set" => args.read_set = Some(input.parse()?),
                        "write_set" => args.write_set = Some(input.parse()?),
                        // Declarative boundary model (#28) — the per-site knob. The
                        // value is the bare enum-variant identifier (`Execute` |
                        // `Substitute`).
                        "replay" => args.replay = Some(input.parse()?),
                        "effect" => args.effect = Some(input.parse()?),
                        "op" => args.op = Some(input.parse()?),
                        "returns" => args.returns = Some(input.parse()?),
                        "correlation" => args.correlation = Some(input.parse()?),
                        // Removed aliases: one name per axis. Each keeps a precise
                        // diagnostic so a stale site tells its author the exact
                        // canonical spelling instead of a generic parse error.
                        removed @ ("trait_name" | "method_name" | "replay_codec"
                        | "result_codec" | "recon" | "replay_strategy"
                        | "effect_kind" | "op_kind" | "return_kind" | "codec_ref"
                        | "state_read_to" | "state_write_to" | "state_touch_to"
                        | "correlation_id") => {
                            let canonical = match removed {
                                "trait_name" => "component",
                                "method_name" => "operation",
                                "replay_codec" | "result_codec" | "recon" => "codec",
                                "replay_strategy" => "replay",
                                "effect_kind" => "effect",
                                "op_kind" => "op",
                                "return_kind" => "returns",
                                "state_read_to" => "state_read",
                                "state_write_to" => "state_write",
                                "state_touch_to" => "state_touch",
                                "correlation_id" => "correlation",
                                // `codec_ref` set non-routing CodecRef metadata that
                                // nothing consumed; there is no replacement.
                                _ => "",
                            };
                            let message = if canonical.is_empty() {
                                format!("`{removed}` was removed and has no replacement")
                            } else {
                                format!("`{removed}` was removed; use `{canonical} = …`")
                            };
                            return Err(syn::Error::new(key.span(), message));
                        }
                        "future" => {
                            let value: LitStr = input.parse()?;
                            match value.value().as_str() {
                                "boxed" => args.future = Some(FutureMode::Boxed),
                                _ => {
                                    return Err(syn::Error::new(
                                        value.span(),
                                        "unsupported future mode; expected `future = \"boxed\"`",
                                    ));
                                }
                            }
                        }
                        _ => {
                            return Err(syn::Error::new(
                                key.span(),
                                "unsupported deja instrument argument",
                            ));
                        }
                    }
                }
            }

            if input.peek(Token![,]) {
                input.parse::<Token![,]>()?;
            }
        }

        Ok(args)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ret(ts: proc_macro2::TokenStream) -> syn::ReturnType {
        syn::parse2(quote!(-> #ts)).expect("return type")
    }

    /// The reconstruct-target extraction for a `future = "boxed"` body must peel
    /// `Pin<Box<dyn Future<Output = X> + ...>>` down to `X` — the type
    /// `dispatch_async` resolves to and the reconstruct closure deserializes into.
    /// Getting this wrong is what made the boxed shape emit
    /// `Option<Pin<Box<..>>>` and fail to type-check.
    #[test]
    fn boxed_future_output_is_the_inner_type() {
        let output = ret(quote!(
            ::core::pin::Pin<Box<dyn ::core::future::Future<Output = Result<u64, String>> + Send>>
        ));
        let inner = boxed_future_output_ty(&output).expect("inner output type");
        assert_eq!(inner.to_string(), quote!(Result<u64, String>).to_string());
    }

    /// A non-future return type has no boxed-future output to extract.
    #[test]
    fn non_future_return_has_no_boxed_output() {
        assert!(boxed_future_output_ty(&ret(quote!(u64))).is_none());
    }

    /// `codec = ResultOkCodec` reaches the right `Result` Ok type even through
    /// the boxed shape: `Pin<Box<dyn Future<Output = CustomResult<R, E>>>>` → `R`.
    #[test]
    fn result_ok_codec_ok_type_through_boxed_future() {
        let output = ret(quote!(
            ::core::pin::Pin<Box<dyn ::core::future::Future<Output = CustomResult<MyRow, MyErr>>>>
        ));
        let ok = first_generic_arg_of_output(&output, Some(FutureMode::Boxed))
            .expect("ok type via boxed");
        assert_eq!(ok.to_string(), quote!(MyRow).to_string());
    }

    /// For a plain (non-boxed) `Result`-like return, the Ok type is its first
    /// generic argument.
    #[test]
    fn result_ok_codec_ok_type_for_plain_return() {
        let output = ret(quote!(CustomResult<MyRow, MyErr>));
        let ok = first_generic_arg_of_output(&output, None).expect("ok type");
        assert_eq!(ok.to_string(), quote!(MyRow).to_string());
    }

    /// The macro expands (does not error) for sync, async, and boxed shapes, and
    /// the emitted tokens route through the single `dispatch` / `dispatch_async`
    /// seam — naming NONE of the removed replay-only operations.
    #[test]
    fn generated_shapes_call_the_single_seam_and_name_no_replay_ops() {
        let cases: [proc_macro2::TokenStream; 3] = [
            quote!(
                fn s(x: u64) -> u64 {
                    x + 1
                }
            ),
            quote!(
                async fn a(x: u64) -> u64 {
                    x + 1
                }
            ),
            quote!(
                fn b(x: u64) -> ::core::pin::Pin<Box<dyn ::core::future::Future<Output = u64>>> {
                    Box::pin(async move { x + 1 })
                }
            ),
        ];
        let futures = [None, None, Some(FutureMode::Boxed)];

        for (src, future) in cases.into_iter().zip(futures) {
            let func: ItemFn = syn::parse2(src).expect("parse fn");
            let args = InstrumentArgs {
                future,
                ..InstrumentArgs::default()
            };
            let expanded = generate(args, func).to_string();

            // Routes through the single seam.
            assert!(
                expanded.contains("dispatch"),
                "expansion must call the dispatch seam: {expanded}"
            );
            // Names ZERO replay-only operations (the decoupling test, design §1.2).
            for banned in [
                "replay_boundary",
                "boundary_execute_mode",
                "execute_shadow_peek_boundary",
                "execute_shadow_observe_boundary",
            ] {
                assert!(
                    !expanded.contains(banned),
                    "macro must NOT name the replay-only op `{banned}`: {expanded}"
                );
            }
        }
    }

    // -----------------------------------------------------------------------
    // Declarative boundary model — macro params, presets, RMW compile error.
    // -----------------------------------------------------------------------

    fn parse_args(attr: proc_macro2::TokenStream) -> InstrumentArgs {
        syn::parse2(attr).expect("parse InstrumentArgs")
    }

    fn parse_fn(src: proc_macro2::TokenStream) -> ItemFn {
        syn::parse2(src).expect("parse fn")
    }

    /// The per-site knob (#28) parses and expands into a `with_semantics`
    /// descriptor carrying `ReplayStrategy::Execute`.
    #[test]
    fn replay_strategy_execute_expands_into_with_semantics() {
        let args = parse_args(quote!(boundary = "redis", replay = Execute));
        let func = parse_fn(quote!(
            fn get(key: String) -> u64 {
                0
            }
        ));
        let expanded = generate(args, func).to_string();
        assert!(
            expanded.contains("with_semantics"),
            "an Execute site must emit with_semantics: {expanded}"
        );
        assert!(expanded.contains("ReplayStrategy :: Execute"), "{expanded}");
        // The old taxonomy is gone — never emitted.
        assert!(!expanded.contains("Channel ::"), "{expanded}");
        assert!(!expanded.contains("Effect ::"), "{expanded}");
        assert!(!expanded.contains("Determinism"), "{expanded}");
    }

    /// An explicit `replay = Substitute` also emits a `with_semantics`
    /// descriptor (the knob is declared, even though it equals the default value).
    #[test]
    fn replay_strategy_substitute_expands_into_with_semantics() {
        let args = parse_args(quote!(boundary = "redis", replay = Substitute));
        let func = parse_fn(quote!(
            fn xlen(key: String) -> u64 {
                0
            }
        ));
        let expanded = generate(args, func).to_string();
        assert!(expanded.contains("with_semantics"), "{expanded}");
        assert!(
            expanded.contains("ReplayStrategy :: Substitute"),
            "{expanded}"
        );
    }

    #[test]
    fn typed_declaration_metadata_expands_into_boundary_semantics() {
        let args = parse_args(quote!(
            boundary = "redis",
            replay = Execute,
            effect = Redis,
            op = Read,
            returns = Raw
        ));
        let func = parse_fn(quote!(
            fn get(key: String) -> u64 {
                0
            }
        ));
        let expanded = generate(args, func).to_string();
        assert!(expanded.contains("BoundaryDeclaration"), "{expanded}");
        assert!(expanded.contains("EffectKind :: Redis"), "{expanded}");
        assert!(expanded.contains("OperationKind :: Read"), "{expanded}");
        assert!(expanded.contains("ReturnSemantics :: Raw"), "{expanded}");
        assert!(
            !expanded.contains("CodecRef"),
            "CodecRef metadata is no longer emitted by declarations: {expanded}"
        );
    }

    /// A site that declares NO knob (and no preset) keeps emitting the legacy
    /// `BoundarySpec::new` constructor — byte-identical to before #28.
    #[test]
    fn undeclared_boundary_emits_plain_new() {
        let func = parse_fn(quote!(
            fn get(key: String) -> u64 {
                0
            }
        ));
        let expanded = generate(InstrumentArgs::default(), func).to_string();
        assert!(
            expanded.contains("BoundarySpec :: new"),
            "undeclared boundary must emit BoundarySpec::new: {expanded}"
        );
        assert!(
            !expanded.contains("with_semantics"),
            "undeclared boundary must NOT emit with_semantics: {expanded}"
        );
    }

    /// `codec = <Path>` is THE result-codec selector — one name per axis. The
    /// removed aliases (`replay_codec` / `result_codec` / `recon`) must reject
    /// with a diagnostic that names the canonical spelling, and the legacy
    /// boolean-style replay flags stay unsupported.
    #[test]
    fn codec_selector_is_canonical_and_removed_aliases_name_the_replacement() {
        let args = parse_args(quote!(boundary = "redis", codec = ResultOkCodec));
        let path = args
            .codec
            .as_ref()
            .expect("`codec` must populate the result-codec selector");
        assert_eq!(
            quote!(#path).to_string(),
            quote!(ResultOkCodec).to_string(),
            "`codec` must parse the ResultOkCodec path"
        );

        let func = parse_fn(quote!(
            fn get(k: String) -> CustomResult<u64, E> {
                Ok(0)
            }
        ));
        let expanded = generate(args, func).to_string();
        assert!(
            expanded.contains("from_value"),
            "`codec` must emit the ResultOkCodec replay reconstruction path: {expanded}"
        );

        for (removed, canonical) in [
            ("replay_codec", "codec"),
            ("result_codec", "codec"),
            ("recon", "codec"),
            ("replay_strategy", "replay"),
            ("effect_kind", "effect"),
            ("op_kind", "op"),
            ("return_kind", "returns"),
            ("trait_name", "component"),
            ("method_name", "operation"),
            ("correlation_id", "correlation"),
            ("state_read_to", "state_read"),
            ("state_write_to", "state_write"),
            ("state_touch_to", "state_touch"),
        ] {
            let key = syn::Ident::new(removed, proc_macro2::Span::call_site());
            let attr = quote!(boundary = "redis", #key = ResultOkCodec);
            let err = match syn::parse2::<InstrumentArgs>(attr) {
                Ok(_) => panic!("removed alias `{removed}` must be rejected"),
                Err(err) => err,
            };
            let message = err.to_string();
            assert!(
                message.contains("was removed") && message.contains(canonical),
                "`{removed}` must name canonical `{canonical}` in its diagnostic; got: {message}"
            );
        }

        // `codec_ref` set CodecRef metadata nothing consumed; no replacement.
        let err = match syn::parse2::<InstrumentArgs>(quote!(codec_ref = "x")) {
            Ok(_) => panic!("`codec_ref` must be rejected"),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("has no replacement"),
            "`codec_ref` diagnostic: {err}"
        );

        for (name, legacy) in [
            ("replay", quote!(boundary = "redis", replay)),
            ("replay_ok", quote!(boundary = "redis", replay_ok)),
            (
                "replay_with",
                quote!(boundary = "redis", replay_with = None::<u64>),
            ),
        ] {
            let err = match syn::parse2::<InstrumentArgs>(legacy) {
                Ok(_) => panic!("legacy `{name}` argument must be unsupported"),
                Err(err) => err,
            };
            assert!(
                err.to_string()
                    .contains("unsupported deja instrument argument"),
                "legacy `{name}` must reject through the unsupported-argument path; got: {err}"
            );
        }
    }

    /// PRESETS after #28: `deja::id`/`time`/`http(outgoing)` default to
    /// `ReplayStrategy::Substitute` and carry a descriptive `kind` label; they emit
    /// no Channel/Effect taxonomy. `deja::http(incoming)` declares NOTHING (the
    /// replay driver) → plain `BoundarySpec::new`.
    #[test]
    fn presets_default_to_substitute_with_kind() {
        // id preset.
        let func = parse_fn(quote!(
            fn nonce() -> u64 {
                0
            }
        ));
        let id = generate_with_preset(InstrumentArgs::default(), func, Some("id"), Preset::Id)
            .to_string();
        assert!(
            id.contains("ReplayStrategy :: Substitute"),
            "id strategy: {id}"
        );
        assert!(id.contains("\"id\""), "id kind label: {id}");
        assert!(
            !id.contains("Channel ::") && !id.contains("Effect ::"),
            "no taxonomy: {id}"
        );

        // time preset.
        let func = parse_fn(quote!(
            fn now() -> u64 {
                0
            }
        ));
        let time =
            generate_with_preset(InstrumentArgs::default(), func, Some("time"), Preset::Time)
                .to_string();
        assert!(
            time.contains("ReplayStrategy :: Substitute"),
            "time strategy: {time}"
        );
        assert!(time.contains("\"time\""), "time kind label: {time}");

        // http outgoing preset: Substitute, kind "http".
        let func = parse_fn(quote!(
            fn send() -> u64 {
                0
            }
        ));
        let http = generate_with_preset(
            InstrumentArgs::default(),
            func,
            Some("http_outgoing"),
            Preset::HttpOutgoing,
        )
        .to_string();
        assert!(
            http.contains("ReplayStrategy :: Substitute"),
            "http strategy: {http}"
        );
        assert!(http.contains("\"http\""), "http kind label: {http}");

        // http incoming preset: declares NOTHING (the replay driver) → plain new,
        // but STILL records its event (the boundary tuple is unchanged).
        let func = parse_fn(quote!(
            fn recv() -> u64 {
                0
            }
        ));
        let http_in = generate_with_preset(
            InstrumentArgs::default(),
            func,
            Some("http_incoming"),
            Preset::HttpIncoming,
        )
        .to_string();
        assert!(
            http_in.contains("BoundarySpec :: new"),
            "http_incoming declares nothing → plain new: {http_in}"
        );
        assert!(
            !http_in.contains("with_semantics"),
            "http_incoming must NOT declare a knob: {http_in}"
        );
        assert!(
            http_in.contains("http_incoming"),
            "still records the event: {http_in}"
        );
    }

    /// An EXPLICIT `replay = Execute` overrides a preset's `Substitute`
    /// default (e.g. opting an http boundary into Execute).
    #[test]
    fn explicit_knob_overrides_preset() {
        let args = parse_args(quote!(replay = Execute));
        let func = parse_fn(quote!(
            fn send() -> u64 {
                0
            }
        ));
        let out = generate_with_preset(args, func, Some("http_outgoing"), Preset::HttpOutgoing)
            .to_string();
        assert!(out.contains("ReplayStrategy :: Execute"), "{out}");
        assert!(
            !out.contains("ReplayStrategy :: Substitute"),
            "preset Substitute overridden: {out}"
        );
    }

    /// An unknown `replay_strategy` variant is a `compile_error!`.
    #[test]
    fn unknown_replay_variant_is_a_compile_error() {
        let args = parse_args(quote!(boundary = "redis", replay = Frobnicate));
        let func = parse_fn(quote!(
            fn x(k: String) -> u64 {
                0
            }
        ));
        let out = generate(args, func).to_string();
        assert!(
            out.contains("compile_error"),
            "unknown replay variant must error: {out}"
        );
        assert!(out.contains("unknown replay"), "{out}");
    }

    #[test]
    fn existing_instrument_arg_names_still_parse_after_state_capture_extension() {
        let _ = parse_args(quote!(
            boundary = "b",
            component = "c",
            operation = "o",
            args = ::serde_json::Value::Null,
            result = ::serde_json::Value::Null,
            codec = SerdeCodec,
            replay = Execute,
            effect = Redis,
            op = Read,
            returns = Raw,
            correlation = None::<String>,
            future = "boxed",
            skip(id),
            fields(extra = 42),
            skip_all,
            ret,
            err
        ));

        let _ = parse_args(quote!(
            component = "t",
            operation = "m",
            correlation = Some("cid".to_string())
        ));
    }

    #[test]
    fn state_capture_single_key_args_expand_into_observation_setters() {
        let args = parse_args(quote!(
            boundary = "cache",
            state_read = key.clone(),
            state_write = format!("{}:shadow", key),
            state_touch = touched_key()
        ));
        let func = parse_fn(quote!(
            fn update(key: String) -> u64 {
                0
            }
        ));
        let expanded = generate(args, func).to_string();

        assert!(
            expanded.contains("CrossingObservation :: with_correlation"),
            "{expanded}"
        );
        assert!(expanded.contains("state_read_to"), "{expanded}");
        assert!(expanded.contains("state_write_to"), "{expanded}");
        assert!(expanded.contains("state_touch_to"), "{expanded}");
    }

    #[test]
    fn state_capture_set_args_expand_into_owned_string_sets() {
        let args = parse_args(quote!(
            boundary = "cache",
            read_set = read_keys,
            write_set = vec!["written"]
        ));
        let func = parse_fn(quote!(
            fn batch(read_keys: Vec<&'static str>) -> u64 {
                0
            }
        ));
        let expanded = generate(args, func).to_string();

        assert!(expanded.contains("with_read_set"), "{expanded}");
        assert!(expanded.contains("with_write_set"), "{expanded}");
        assert!(expanded.contains("into_iter"), "{expanded}");
        assert!(expanded.contains("Into :: into"), "{expanded}");
        assert!(expanded.contains("collect :: < Vec < String"), "{expanded}");
    }

    #[test]
    fn state_capture_chaining_is_shared_by_all_generated_shapes() {
        let cases: [(proc_macro2::TokenStream, Option<FutureMode>); 3] = [
            (
                quote!(
                    fn s(x: u64) -> u64 {
                        x + 1
                    }
                ),
                None,
            ),
            (
                quote!(
                    async fn a(x: u64) -> u64 {
                        x + 1
                    }
                ),
                None,
            ),
            (
                quote!(
                    fn b(
                        x: u64,
                    ) -> ::core::pin::Pin<Box<dyn ::core::future::Future<Output = u64>>>
                    {
                        Box::pin(async move { x + 1 })
                    }
                ),
                Some(FutureMode::Boxed),
            ),
        ];

        for (src, future) in cases {
            let func = parse_fn(src);
            let mut args = parse_args(quote!(
                boundary = "cache",
                state_read = "read-key",
                read_set = ["read-key"],
                write_set = ["write-key"]
            ));
            args.future = future;
            let expanded = generate(args, func).to_string();

            assert!(
                expanded.contains("CrossingObservation :: with_correlation"),
                "{expanded}"
            );
            assert!(expanded.contains("state_read_to"), "{expanded}");
            assert!(expanded.contains("with_read_set"), "{expanded}");
            assert!(expanded.contains("with_write_set"), "{expanded}");
            let args_pos = expanded
                .find("__deja_boundary_args")
                .expect("args binding emitted");
            let state_pos = expanded
                .find("state_read_to")
                .expect("state capture emitted");
            assert!(
                args_pos < state_pos,
                "args must materialize before state capture can move keys: {expanded}"
            );
        }
    }

    #[test]
    fn state_capture_canonical_names_parse() {
        let _ = parse_args(quote!(
            state_read = "read-key",
            state_write = "write-key",
            state_touch = "touch-key"
        ));
    }

    #[test]
    fn unsupported_state_argument_still_errors() {
        let err = match syn::parse2::<InstrumentArgs>(quote!(state_reads = "k")) {
            Ok(_) => panic!("unknown state capture spelling must be rejected"),
            Err(err) => err,
        };
        assert!(
            err.to_string()
                .contains("unsupported deja instrument argument"),
            "{err}"
        );
    }
}
