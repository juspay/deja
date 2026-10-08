-- Who proposed, acknowledged or withdrew, by the sign-in issuer's stable id
-- (Google's `sub`) beside the email already kept. An email can be renamed or
-- reassigned to another person; the subject cannot, so the trail and the
-- "not the proposer" rule hold across that.
--
-- Null on a row written while sign-in was off, or by the pipeline's service
-- token. With sign-in on, a proposal without a subject is from before it and
-- cannot be confirmed: it has to be proposed again by a signed-in person.
ALTER TABLE acknowledgements
  ADD COLUMN proposed_by_sub     text,
  ADD COLUMN acknowledged_by_sub text,
  ADD COLUMN withdrawn_by_sub    text;
