-- The delta verdict: what a run changed relative to the baseline it was
-- created against (params.delta_against), three-way with the tape as the
-- ancestor. 'pass' | 'fail' | 'pending' (a side is still running) | 'refused'
-- (the pairing can never give a delta: different tapes or event schemas, a
-- side that finished unscored). NULL for a run that names no baseline. Set by
-- the orchestrator when either run finishes or when the delta is first
-- computed; never read by the tape verdict. Advisory: nothing should gate on
-- it before a same-image replay pair is measured to give an empty delta.
ALTER TABLE replay_runs ADD COLUMN delta_verdict TEXT;
