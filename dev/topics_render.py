"""Render the topics dbt models with plain Jinja for DuckDB (local unit tests and Dev trials)."""
import re
from pathlib import Path
import jinja2
import yaml

ROOT = Path(".")


def render(path, refs, sources, topics, as_of=None):
    text = (ROOT / path).read_text()
    text = re.sub(r"\{\{\s*config\(.*?\)\s*\}\}", "", text)
    env = jinja2.Environment(extensions=["jinja2.ext.do"])
    macro = (ROOT / "warehouse/macros/topics.sql").read_text()
    def setting(p):
        v = topics
        for part in p.split("."):
            v = v[part]
        return v
    def as_of_fn():
        return f"date '{as_of}'" if as_of else "cast(timezone('Europe/Warsaw', now()) as date)"
    ctx = {"ref": lambda n: refs[n], "source": lambda a, b: sources[(a, b)], "topics_setting": setting,
           "topics_as_of": as_of_fn}
    return env.from_string(text).render(**ctx)


def topics_cfg():
    return yaml.safe_load((ROOT / "warehouse/warehouse.yaml").read_text())["topics"]
