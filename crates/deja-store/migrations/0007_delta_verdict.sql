-- The delta verdict: what a run changed relative to the baseline it was
-- created against (params.delta_against), three-way with the tape as the
-- ancestor. 'pass' | 'fail' | 'pending' (baseline not scored yet). NULL for a
-- run that names no baseline. Set by the orchestrator when the run's result
-- lands or when the delta is first computed; never read by the tape verdict.
ALTER TABLE replay_runs ADD COLUMN delta_verdict TEXT;
