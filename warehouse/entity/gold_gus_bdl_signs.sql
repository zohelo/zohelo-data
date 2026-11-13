-- GUS BDL no-value signs (docs/gold-coverage.md; #2788, #2806, #2852), kept as the table `signs` for the
-- steps of the build stage: a 0 under one of these is a cell without a value and is not loaded; a number
-- other than 0 under such a sign is kept. The website export carries the sign, the API its attribute id
-- (/attributes: 4 and 91 = x, 14 = X, 50 = n).
--   x, X   "brak informacji, konieczność zachowania tajemnicy statystycznej lub wypełnienie pozycji jest
--          niemożliwe albo niecelowe"
--   n      "dana jeszcze niedostępna"
-- Not here, so kept as published: z/Z (a significant zero), a/A (below the presentation format) and the
-- API's id 0 (blank sign).
select
    t.channel,
    t.sign
from (
    values
    ('web_export', 'x'),
    ('web_export', 'n'),
    ('api', '4'),
    ('api', '14'),
    ('api', '50'),
    ('api', '91')
) as t (channel, sign)
