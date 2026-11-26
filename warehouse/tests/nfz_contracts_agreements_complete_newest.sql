{{ config(severity='warn') }}
-- NFZ contracts (register PLS-014): the newest landed contract year has agreements of each of the 16 NFZ branches.
-- Only a warning: when /available-years adds a year, a branch may have no agreement yet (a valid empty list), and a
-- run that stops on its time budget leaves entities for the next run. Gaps in earlier years fail
-- (nfz_contracts_agreements_complete.sql).
{{ poc_nfz_contracts_gaps('newest') }}
