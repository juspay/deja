//! k8s Job executor — the control plane's "launch a replay as a Job" seam.
//!
//! Design (docs/design/incluster-deployment-plan.md, Amendment 1): the
//! orchestrator is a THIN interface. It does NOT render a Job from scratch in
//! Rust. The Job template — service account, RBAC, sidecars, volumes, resource
//! limits, security context, the candidate's boot guard — is authored once in
//! the env profile (the `replay-env` chart's ConfigMap) and owned by ArgoCD.
//! The orchestrator GETs that template and applies a small, TYPED patch: the
//! per-run fields (name, labels, candidate image, a handful of env vars), then
//! POSTs the result. This keeps environment shape out of the binary — you change
//! the environment by editing the chart, not by shipping a new orchestrator.
//!
//! This module is the patch itself: a pure `serde_json::Value → Value` overlay,
//! unit-testable with no cluster. The REST transport (GET template, POST job,
//! watch status) layers on top and is the only part that needs a live API.

mod config;
mod env;
mod k8s;
mod launch;
mod patch;
pub mod reconcile;

pub use config::{resolve_candidate_image, ExecutorKind, K8sExecutorConfig};
pub use env::{runner_env, CandidateBinding};
pub use k8s::{
    job_terminal_verdict, InClusterConfig, KubeApi, KubeError, KubeRequest, KubeResponse,
    KubeTransport, UreqTransport,
};
pub use launch::{
    build_job, job_name_for, launch, launch_spec_for_run, watch_to_terminal, ExecutorError,
    LaunchSpec, RUN_ID_LABEL,
};
pub use patch::{apply_job_patch, EnvUpsert, JobPatch, PatchError};
