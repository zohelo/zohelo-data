"""Read-only probe (deleted before merge): silver IDS tables for the Gold block design."""

from dev.src_gold_probe import connect, show


def main() -> int:
    con = connect()
    o = "r2.silver.ids_observations"
    show(con, "ids tables", "select table_schema, table_name from information_schema.tables where table_name like 'ids_%'")
    show(con, "ids rows", f"select count(*), max(source_version), min(year), max(year) from {o}")
    show(con, "ids counterparts", f"select counterpart_code, any_value(counterpart_name), count(*) from {o} group by 1 order by 3 desc", 40)
    show(con, "ids series pick", f"""select series_code, any_value(series_name), count(*), count(distinct economy_code), min(year), max(year),
        count(*) filter (where value is null), count(distinct counterpart_code)
        from {o} where series_code in ('DT.DOD.DECT.CD','DT.TDS.DECT.CD','DT.DOD.DECT.GN.ZS','DT.TDS.DECT.GN.ZS',
        'DT.TDS.DECT.EX.ZS','DT.DOD.DECT.EX.ZS','DT.DOD.DLXF.CD','DT.DOD.DSTC.CD','DT.INT.DECT.CD','DT.DOD.DSTC.IR.ZS')
        group by 1 order by 1""")
    show(con, "ids economies", f"""select string_agg(distinct o.economy_code, ',' order by o.economy_code) filter (where w.economy_code is null),
        count(distinct o.economy_code), count(distinct o.economy_code) filter (where w.iso2_code is not null and w.region is not null)
        from {o} as o left join r2.silver.wdi_countries as w on o.economy_code = w.economy_code""")
    show(con, "ids europe", f"""select o.economy_code, any_value(o.economy_name), count(*), max(o.year) from {o} as o
        where o.series_code = 'DT.DOD.DECT.CD' and o.economy_code in ('UKR','TUR','SRB','ALB','MKD','BIH','MNE','MDA','GEO','ARM','BLR','RUS','KAZ','XKX','ECA','LMY','LIC','MIC')
        group by 1 order by 1""", 30)
    show(con, "ids projections", f"""select series_code, max(year) filter (where value is not null), count(*) filter (where year > 2025)
        from {o} where economy_code = 'UKR' and series_code in ('DT.DOD.DECT.CD','DT.TDS.DECT.CD','DT.TDS.DECT.GN.ZS') and counterpart_code = 'WLD' group by 1""")
    show(con, "ids ukr latest", f"""select series_code, counterpart_code, year, value from {o} where economy_code = 'UKR'
        and series_code in ('DT.DOD.DECT.CD','DT.TDS.DECT.CD','DT.DOD.DECT.GN.ZS','DT.TDS.DECT.GN.ZS') and year >= 2022 order by 1, 2, 3""", 40)
    show(con, "ids series meta", "select series_code, in_series_metadata, series_name from r2.silver.ids_series where series_code like 'DT.TDS.DECT%' or series_code like 'DT.DOD.DECT%'", 30)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
