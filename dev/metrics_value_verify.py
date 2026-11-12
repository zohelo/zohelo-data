"""Read-only proof for #2788: the new tests/metrics_value_range.sql run on prod Gold metrics, its
remaining rows split by whether the fixed build stages leave them out (GUS no-value signs), and
the values the fixed filters keep for the affected GUS measures. No writes."""

from pathlib import Path
import re

from dev.metrics_value_zeros import connect, q
from ingestion.common.report import annotate


def main() -> int:
    con = connect()
    test = Path("warehouse/tests/metrics_value_range.sql").read_text()
    test = re.sub(r"\{\{ ref\('poc_(metrics_\w+)'\) \}\}", r"r2.gold.\1", test)
    test = "\n".join(line for line in test.splitlines() if not line.lstrip().startswith("--"))
    out = {
        "test_rows_by_measure": q(
            con,
            f"""select source_system, measure_key, count(*), min(value), max(value),
                       count(*) filter (where value = 0)
                from ({test}) group by all order by all""",
        ),
        "bound_overlaps": q(
            con,
            test.split("metrics as (")[0].rstrip().rstrip(",")
            + """ select b1.concept_key, b1.source_system from bounds b1 join bounds b2
                  on b1.concept_key=b2.concept_key and b1.source_system=b2.source_system
                  and b1.measure_key_pattern<b2.measure_key_pattern
                  and (b1.measure_key_pattern='%' or b2.measure_key_pattern='%')""",
        ),
        "dbw_kept": q(
            con,
            """select regexp_replace(entity_id, '(_[0-9]{4})?_pl$', '') b, presentation_id, count(*), min(value),
                      max(value), count(*) filter (where value = 0)
               from r2.silver.dbw_observations
               where (entity_id like '1038_16_389_3%' or entity_id like '1038_16_92_4%'
                      or entity_id like '1514_1360_95_3%' or entity_id like '1038_16_105_4%')
                 and value is not null and coalesce(missing_reason_id, 253) = 253
               group by all order by all""",
        ),
        "dbw_dropped_activated": q(
            con,
            """select missing_reason_id, count(*), count(*) filter (where value <> 0)
               from r2.silver.dbw_observations
               where value is not null and coalesce(missing_reason_id, 253) <> 253
                 and regexp_replace(entity_id, '(_[0-9]{4})?_pl$', '') in (
                   '581_57_93_4','581_16_2_1','305_739_5_1','305_1722_5_1','305_1721_5_3','376_2_243_1',
                   '376_2_184_1','376_16_182_1','398_16_243_3','398_16_184_3','398_16_182_3','875_143_95_1',
                   '1514_1360_95_3','1513_1360_95_3','1512_1360_95_3','1038_16_389_3','1038_16_390_3',
                   '1038_16_92_4','1038_16_105_4','1173_793_92_4','814_2_7_1','392_16_223_1','109_849_7_1',
                   '469_16_117_1')
               group by all""",
        ),
        "bdl_p2361_kept": q(
            con,
            """select count(*), min(value), max(value), count(*) filter (where value = 0)
               from r2.silver.bdl_observations
               where subgroup_id = 'P2361' and value is not null and year >= 2010
                 and coalesce(lower(attribute), '') <> 'x'""",
        ),
    }
    annotate("notice", "metrics_value_range verify", out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
