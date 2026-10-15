-- The pipeline's pause switch, one row per system. While `paused`, the
-- Jenkins jobs that act (integ-deploy, hop-replay, cut-release, promote)
-- stop before their next action; reconcile keeps reporting, since it only
-- reads. Only a signed-in maintainer flips it, with a reason; each flip is
-- also written to the audit log, so this row is the current state and the
-- audit is its history. No row means running.
CREATE TABLE cd_controls (
  system         text PRIMARY KEY,
  paused         boolean NOT NULL DEFAULT false,
  reason         text,
  changed_by     text NOT NULL,
  changed_by_sub text,
  changed_at     timestamptz NOT NULL DEFAULT now()
);
