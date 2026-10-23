-- Immutable, per-system replay argument policies. A proposal is not active
-- until a different signed-in maintainer confirms it. The active pointer and
-- its audit event are written in one transaction; rollback re-selects an old
-- revision rather than mutating its document.
CREATE TABLE boundary_policy_revisions (
    id BIGSERIAL PRIMARY KEY,
    system TEXT NOT NULL,
    digest TEXT NOT NULL,
    document JSONB NOT NULL,
    reason TEXT NOT NULL,
    proposed_by TEXT NOT NULL,
    proposed_by_sub TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    confirmed_by TEXT,
    confirmed_by_sub TEXT,
    activated_at TIMESTAMPTZ,
    CHECK (length(btrim(system)) > 0),
    CHECK (length(btrim(reason)) > 0),
    CHECK (length(btrim(proposed_by_sub)) > 0)
);
-- Digest identifies scoring semantics, not a unique proposal: different
-- maintainers can independently propose the same rules and retain authorship.
CREATE INDEX boundary_policy_revisions_system_id
    ON boundary_policy_revisions (system, id DESC);

CREATE TABLE boundary_policy_active (
    system TEXT PRIMARY KEY,
    revision_id BIGINT NOT NULL REFERENCES boundary_policy_revisions(id),
    changed_by TEXT NOT NULL,
    changed_by_sub TEXT NOT NULL,
    changed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
