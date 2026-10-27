"""Dev trial (deleted before merge): landing_objects with a non-prefix argument and a missing key."""

from ops import landing_objects

if __name__ == "__main__":
    raise SystemExit(landing_objects.main(["01_landing/nbp", "01_landing/no_such_source/x.zip"]))
