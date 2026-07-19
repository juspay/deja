//! The typed Job-template patch: a pure overlay of per-run fields onto a Job
//! spec fetched from a ConfigMap. Everything here is fail-loud — a patch that
//! silently matches nothing (a renamed container, a missing env array) is how a
//! Job ends up running the template's default image with none of the run's env,
//! producing a confidently wrong verdict. Every target that cannot be found is
//! an error, never a no-op.

use serde_json::Value;

/// A per-run overlay onto a Job template. Deliberately small: only the fields
/// that vary run-to-run. Anything structural (SA, volumes, sidecars, limits)
/// stays in the template.
#[derive(Debug, Clone, Default)]
pub struct JobPatch {
    /// `metadata.name` for the Job.
    pub job_name: String,
    /// Labels merged into BOTH `metadata.labels` and the pod template's
    /// `spec.template.metadata.labels` (so selectors and the Job match).
    pub labels: Vec<(String, String)>,
    /// Per-container image overrides: `(container_name, image)`. A named
    /// container that is absent is an error.
    pub images: Vec<(String, String)>,
    /// Per-container env upserts. The executor computes these — the runner's own
    /// env AND the candidate's binding (which env var maps to which artifact,
    /// read from the env profile) — so this type carries no candidate-specific
    /// names and stays generic.
    pub env: Vec<EnvUpsert>,
}

/// Upsert one env var on one container: replace the entry with this `name` if
/// present, else append it. Keyed by `(container, name)`.
#[derive(Debug, Clone)]
pub struct EnvUpsert {
    pub container: String,
    pub name: String,
    pub value: String,
}

impl EnvUpsert {
    pub fn new(
        container: impl Into<String>,
        name: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        Self {
            container: container.into(),
            name: name.into(),
            value: value.into(),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum PatchError {
    /// The template is not shaped like a Job (missing the path we must patch).
    Shape(String),
    /// A patch names a container the template does not define.
    ContainerNotFound { container: String },
}

impl std::fmt::Display for PatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PatchError::Shape(p) => write!(f, "job template not shaped as expected: {p}"),
            PatchError::ContainerNotFound { container } => write!(
                f,
                "patch targets container '{container}' but the template defines no such container"
            ),
        }
    }
}

impl std::error::Error for PatchError {}

/// Apply the patch, returning a new Job value. The input template is not
/// mutated. Fails loudly if any target is absent.
pub fn apply_job_patch(template: &Value, patch: &JobPatch) -> Result<Value, PatchError> {
    let mut job = template.clone();

    // metadata.name
    let metadata = job
        .get_mut("metadata")
        .and_then(Value::as_object_mut)
        .ok_or_else(|| PatchError::Shape("metadata".into()))?;
    metadata.insert("name".into(), Value::String(patch.job_name.clone()));

    // metadata.labels (create the map if absent)
    merge_labels(
        metadata
            .entry("labels")
            .or_insert_with(|| Value::Object(Default::default())),
        &patch.labels,
    )?;

    // spec.template.metadata.labels — the pod labels the Job's selector matches.
    {
        let pod_meta = job
            .pointer_mut("/spec/template/metadata")
            .ok_or_else(|| PatchError::Shape("spec.template.metadata".into()))?;
        let pod_meta = pod_meta
            .as_object_mut()
            .ok_or_else(|| PatchError::Shape("spec.template.metadata".into()))?;
        merge_labels(
            pod_meta
                .entry("labels")
                .or_insert_with(|| Value::Object(Default::default())),
            &patch.labels,
        )?;
    }

    // Image + env target a named container, which may be a main container OR an
    // initContainer (e.g. the `migrations` init that pulls the CodeBundle by
    // sha). Both arrays are searched; a name in neither is a loud error.
    if patch.images.is_empty() && patch.env.is_empty() {
        return Ok(job);
    }

    for (name, image) in &patch.images {
        let c = find_container_in_job(&mut job, name)?;
        c.insert("image".into(), Value::String(image.clone()));
    }

    for up in &patch.env {
        let c = find_container_in_job(&mut job, &up.container)?;
        let env = c
            .entry("env")
            .or_insert_with(|| Value::Array(Vec::new()))
            .as_array_mut()
            .ok_or_else(|| PatchError::Shape(format!("container '{}' env is not a list", up.container)))?;
        upsert_env(env, &up.name, &up.value);
    }

    Ok(job)
}

fn merge_labels(target: &mut Value, labels: &[(String, String)]) -> Result<(), PatchError> {
    let map = target
        .as_object_mut()
        .ok_or_else(|| PatchError::Shape("labels is not a map".into()))?;
    for (k, v) in labels {
        map.insert(k.clone(), Value::String(v.clone()));
    }
    Ok(())
}

/// Find a container by name across BOTH the pod's `containers` and its
/// `initContainers`, returning a mutable handle. initContainers are included so
/// a per-run patch can reach e.g. the migrations init that pulls the CodeBundle.
/// Two-step (locate the array immutably, then fetch mutably) to satisfy the
/// borrow checker without cloning.
fn find_container_in_job<'a>(
    job: &'a mut Value,
    name: &str,
) -> Result<&'a mut serde_json::Map<String, Value>, PatchError> {
    const ARRAYS: [&str; 2] = [
        "/spec/template/spec/containers",
        "/spec/template/spec/initContainers",
    ];
    let mut target: Option<&'static str> = None;
    for path in ARRAYS {
        let here = job
            .pointer(path)
            .and_then(Value::as_array)
            .map(|arr| {
                arr.iter()
                    .any(|c| c.get("name").and_then(Value::as_str) == Some(name))
            })
            .unwrap_or(false);
        if here {
            target = Some(path);
            break;
        }
    }
    let path = target.ok_or_else(|| PatchError::ContainerNotFound {
        container: name.to_owned(),
    })?;
    job.pointer_mut(path)
        .and_then(Value::as_array_mut)
        .and_then(|arr| {
            arr.iter_mut()
                .find(|c| c.get("name").and_then(Value::as_str) == Some(name))
        })
        .and_then(Value::as_object_mut)
        .ok_or_else(|| PatchError::ContainerNotFound {
            container: name.to_owned(),
        })
}

/// Replace the `{name,value}` entry with this name, or append it. k8s env is an
/// ordered list of `{name, value}`; a duplicate name is undefined, so upsert.
fn upsert_env(env: &mut Vec<Value>, name: &str, value: &str) {
    let entry = serde_json::json!({ "name": name, "value": value });
    if let Some(existing) = env
        .iter_mut()
        .find(|e| e.get("name").and_then(Value::as_str) == Some(name))
    {
        *existing = entry;
    } else {
        env.push(entry);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn template() -> Value {
        json!({
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": { "labels": { "app": "replay" } },
            "spec": {
                "template": {
                    "metadata": { "labels": { "app": "replay" } },
                    "spec": {
                        "containers": [
                            { "name": "runner", "image": "deja-orchestrator:template",
                              "env": [ { "name": "DEJA_RUN_ID", "value": "PLACEHOLDER" } ] },
                            { "name": "candidate", "image": "candidate:template" }
                        ]
                    }
                }
            }
        })
    }

    #[test]
    fn patches_name_labels_image_and_upserts_env() {
        let patch = JobPatch {
            job_name: "deja-replay-run7".into(),
            labels: vec![("deja.run-id".into(), "run7".into())],
            images: vec![("candidate".into(), "candidate:sha_c".into())],
            env: vec![
                // upsert existing
                EnvUpsert::new("runner", "DEJA_RUN_ID", "run7"),
                // append new
                EnvUpsert::new("runner", "RUNNER_EXPECTED_MIGRATIONS", "0001\n0002"),
                // env on a container that had none
                EnvUpsert::new("candidate", "ROUTER__DEJA__MODE", "replay"),
            ],
        };
        let out = apply_job_patch(&template(), &patch).expect("patch applies");

        assert_eq!(out["metadata"]["name"], json!("deja-replay-run7"));
        assert_eq!(out["metadata"]["labels"]["deja.run-id"], json!("run7"));
        // pre-existing label preserved
        assert_eq!(out["metadata"]["labels"]["app"], json!("replay"));
        // pod template labels also carry the run id (selector match)
        assert_eq!(
            out["spec"]["template"]["metadata"]["labels"]["deja.run-id"],
            json!("run7")
        );

        let containers = out["spec"]["template"]["spec"]["containers"]
            .as_array()
            .expect("containers array");
        let runner = &containers[0];
        let candidate = &containers[1];
        assert_eq!(candidate["image"], json!("candidate:sha_c"));

        // DEJA_RUN_ID replaced in place, not duplicated.
        let runner_env = runner["env"].as_array().expect("runner env array");
        let run_ids: Vec<_> = runner_env
            .iter()
            .filter(|e| e["name"] == json!("DEJA_RUN_ID"))
            .collect();
        assert_eq!(run_ids.len(), 1, "no duplicate env name");
        assert_eq!(run_ids[0]["value"], json!("run7"));
        // new env appended
        assert!(runner_env
            .iter()
            .any(|e| e["name"] == json!("RUNNER_EXPECTED_MIGRATIONS")
                && e["value"] == json!("0001\n0002")));
        // candidate got a fresh env list
        assert_eq!(candidate["env"][0]["name"], json!("ROUTER__DEJA__MODE"));
    }

    #[test]
    fn env_can_target_an_init_container() {
        // The CodeBundle URI must reach the `migrations` initContainer (Option B),
        // not a main container.
        let mut tmpl = template();
        tmpl["spec"]["template"]["spec"]["initContainers"] =
            json!([{ "name": "migrations", "image": "runner:tmpl" }]);
        let patch = JobPatch {
            job_name: "j".into(),
            env: vec![EnvUpsert::new(
                "migrations",
                "DEJA_CODE_BUNDLE_URI",
                "s3://art/codebundles/ff191d7f/migrations.tar",
            )],
            ..Default::default()
        };
        let out = apply_job_patch(&tmpl, &patch).expect("patch applies to init container");
        let inits = out["spec"]["template"]["spec"]["initContainers"]
            .as_array()
            .expect("initContainers array");
        assert_eq!(inits[0]["env"][0]["name"], json!("DEJA_CODE_BUNDLE_URI"));
        assert_eq!(
            inits[0]["env"][0]["value"],
            json!("s3://art/codebundles/ff191d7f/migrations.tar")
        );
    }

    #[test]
    fn missing_container_is_a_loud_error_not_a_noop() {
        let patch = JobPatch {
            job_name: "j".into(),
            images: vec![("router".into(), "x:y".into())], // template calls it "candidate"
            ..Default::default()
        };
        let err = apply_job_patch(&template(), &patch).expect_err("must reject missing container");
        assert_eq!(
            err,
            PatchError::ContainerNotFound {
                container: "router".into()
            }
        );
    }

    #[test]
    fn env_on_missing_container_is_also_loud() {
        let patch = JobPatch {
            job_name: "j".into(),
            env: vec![EnvUpsert::new("nope", "K", "V")],
            ..Default::default()
        };
        assert!(matches!(
            apply_job_patch(&template(), &patch),
            Err(PatchError::ContainerNotFound { .. })
        ));
    }

    #[test]
    fn non_job_shape_is_rejected() {
        let not_a_job = json!({ "hello": "world" });
        let patch = JobPatch {
            job_name: "j".into(),
            ..Default::default()
        };
        assert!(matches!(
            apply_job_patch(&not_a_job, &patch),
            Err(PatchError::Shape(_))
        ));
    }

    #[test]
    fn label_only_patch_does_not_require_containers() {
        // A template with no containers array still takes a name/label patch —
        // we only demand the containers path when an image/env patch needs it.
        let bare = json!({
            "metadata": {},
            "spec": { "template": { "metadata": {} } }
        });
        let patch = JobPatch {
            job_name: "bare".into(),
            labels: vec![("k".into(), "v".into())],
            ..Default::default()
        };
        let out = apply_job_patch(&bare, &patch).expect("label-only patch applies");
        assert_eq!(out["metadata"]["name"], json!("bare"));
        assert_eq!(out["spec"]["template"]["metadata"]["labels"]["k"], json!("v"));
    }
}
