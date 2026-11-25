-- NFZ contracts (register PLS-014): every contract year from the first year in scope (poc_nfz_contracts_first_year,
-- 2019) up to the year before the newest landed year has agreements of each of the 16 NFZ regional branches. A year
-- or branch missing from Landing, or rejected by Bronze, returns a row. The newest year: ..._complete_newest.sql.
{{ poc_nfz_contracts_gaps('earlier') }}
