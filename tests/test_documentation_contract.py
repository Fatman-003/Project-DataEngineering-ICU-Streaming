"""Guard the published documentation against drift from runtime contracts."""
from pathlib import Path
import re
import sys
import unittest

PROJECT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(PROJECT / "jobs/flink"), str(PROJECT / "services/storage_writer")]

from contracts import CORE_TOPICS
from config import CORE_TABLES, GROUP_ID as WRITER_GROUP, TOPIC_TABLE


def read(relative):
    return (PROJECT / relative).read_text(encoding="utf-8-sig")


class DocumentationContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.readme = read("README.md")
        cls.flow = read("KAFKA_FLINK_FLOW.md")
        cls.design = read("docs/DATABASE_DESIGN.md")
        cls.schema = read("docker/timescaledb/init/001_schema.sql")
        cls.hyper = read("docker/timescaledb/init/002_hypertables.sql")
        cls.indexes = read("docker/timescaledb/init/003_indexes.sql")
        cls.compose = read("docker/flink/compose.yaml")

    def test_readme_commands_ports_and_private_boundary(self):
        self.assertTrue((PROJECT / "run_icu_demo.ps1").is_file())
        self.assertIn(".\\run_icu_demo.ps1", self.readme)
        self.assertIn(".\\run_icu_demo.ps1 -FastVerify", self.readme)
        self.assertIn("-KeepDatabaseData", self.readme)
        for port in re.findall(r'"127\.0\.0\.1:(\d+):\d+"', self.compose):
            self.assertIn("localhost:" + port, self.readme)
        self.assertIn("localhost:9092", self.readme)
        for key in ("POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD"):
            self.assertIn(key, self.readme)
        self.assertIn("icu.v2.deadletter", self.readme)
        self.assertNotIn("icu_local_demo_only_2026", self.readme)
        for text in (self.readme, self.flow, self.design):
            self.assertNotRegex(text, r"(?i)TICKET[- ]?0\d+|PLANNED\s*[—-]\s*TICKET|database not implemented|future Storage Writer")
        self.assertNotRegex(self.readme, r"(?i)completion report|tests passed|next ticket|manual verification pending")

    def test_flow_lists_exact_topics_groups_and_storage_mapping(self):
        topic_section = self.flow.split("## The seven topics", 1)[1].split("## What Flink does", 1)[0]
        documented_topics = set(re.findall(r"^\| `(icu\.v2\.[^`]+)` \|", topic_section, re.M))
        self.assertEqual(documented_topics, set(CORE_TOPICS))
        group_source = read("services/realtime_gateway/gateway.py")
        gateway_group = re.search(r'^GROUP_ID = "([^"]+)"', group_source, re.M).group(1)
        self.assertIn(gateway_group, self.flow)
        self.assertIn(WRITER_GROUP, self.flow)
        mapping_section = self.flow.split("| Kafka business topic | TimescaleDB business hypertable", 1)[1]
        documented_mapping = dict(re.findall(r"^\| `(icu\.v2\.[^`]+)` \| `([a-z_0-9]+)` \|", mapping_section, re.M))
        self.assertEqual(documented_mapping, TOPIC_TABLE)
        self.assertIn("point-in-time", self.flow)
        self.assertIn("ws://localhost:8083/ws", self.flow)

    def test_design_tables_hypertables_and_physical_keys_match_sql(self):
        schema_tables = set(re.findall(r"CREATE TABLE IF NOT EXISTS (\w+)", self.schema))
        self.assertEqual(schema_tables, set(CORE_TABLES))
        refs_section = self.design.split("## References and relationships", 1)[1].split("## Business hypertables", 1)[0]
        business_section = self.design.split("## Business hypertables", 1)[1].split("### Access indexes", 1)[0]
        refs = set(re.findall(r"^\| `([a-z_]+)` \|", refs_section, re.M))
        business = set(re.findall(r"^\| `([a-z_0-9]+)`, `\w+ TIMESTAMPTZ`", business_section, re.M))
        self.assertEqual(refs | business, schema_tables)
        self.assertEqual(len(refs), 5)
        self.assertEqual(len(business), 4)
        hypertables = dict(re.findall(r"create_hypertable\('(\w+)', by_range\('(\w+)'\)", self.hyper))
        self.assertEqual(set(hypertables), business)
        for table, time_column in hypertables.items():
            self.assertIn(f"`{table}`, `{time_column} TIMESTAMPTZ`", self.design)
            ddl = re.search(rf"CREATE TABLE IF NOT EXISTS {table} \((.*?)\);", self.schema, re.S).group(1)
            physical_key = re.search(r"PRIMARY KEY \(([^)]+)\)", ddl).group(1)
            self.assertIn(f"`({physical_key})`", business_section)
        self.assertIn("time-based", self.design)
        self.assertIn("AT_LEAST_ONCE", self.design)

    def test_design_indexes_and_image_match_sources(self):
        image = re.search(r"image: (timescale/timescaledb:[^\s]+)", self.compose).group(1)
        self.assertIn(image, self.design)
        actual = re.findall(r"CREATE INDEX IF NOT EXISTS (\w+) ON (\w+) (\([^)]+\));", self.indexes)
        documented = re.findall(r"^\| `(\w+_idx)` \| `(\w+)` \| `(\([^)]+\))` \|", self.design, re.M)
        self.assertEqual(set(documented), set(actual))
        self.assertEqual(len(actual), 9)
        self.assertIn("PostgreSQL `17.11`", self.design)
        self.assertIn("TimescaleDB extension `2.30.2`", self.design)


if __name__ == "__main__":
    unittest.main()
