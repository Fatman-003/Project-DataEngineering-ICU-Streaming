"""Canonical team documentation agrees with current contracts and reference IDs."""
import re
import unittest
from pathlib import Path
PROJECT=Path(__file__).resolve().parents[1]

class DocumentationContractTests(unittest.TestCase):
    def test_canonical_team_guides_remain_available(self):
        for p in ("docs/KAFKA_MESSAGE_FORMATS.md","README.md","KAFKA_FLINK_FLOW.md",
                  "docs/DATABASE_DESIGN.md","docs/ALERT_RULES_AND_FALSE_ALARM_MANAGEMENT.md"):
            self.assertTrue((PROJECT/p).is_file())

    def test_business_topics_match_canonical_contract(self):
        from contracts import BUSINESS_TOPICS,DLQ_TOPIC
        text=(PROJECT/"docs/KAFKA_MESSAGE_FORMATS.md").read_text(encoding="utf-8")
        for topic in (*BUSINESS_TOPICS.values(),DLQ_TOPIC): self.assertIn("`"+topic+"`",text)

    def test_target_business_columns_and_hypertables_match_documentation(self):
        from db import TABLE_COLUMNS
        text=(PROJECT/"docs/DATABASE_DESIGN.md").read_text(encoding="utf-8")
        for table,columns in TABLE_COLUMNS.items():
            ddl=re.search(r"CREATE TABLE IF NOT EXISTS "+table+r" \((.*?)\);",text,re.S).group(1)
            for field in columns: self.assertRegex(ddl,r"\b"+field+r"\b")
        sql=(PROJECT/"docker/timescaledb/init/002_hypertables.sql").read_text()
        self.assertEqual(sql.count("create_hypertable"),4)
        self.assertNotIn("'chartevents'",sql)
        self.assertNotIn("'labevents'",sql)
