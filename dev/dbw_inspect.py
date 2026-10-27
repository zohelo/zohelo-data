"""Dev trial (deleted before merge): GUS DBW landing inspect (read-only) shows the excluded indicator."""

from ingestion.to_landing.gus_dbw import extract

if __name__ == "__main__":
    raise SystemExit(extract.main(["--mode", "inspect", "--budget-minutes", "20"]))
