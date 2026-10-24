"""Proposals are implementation references; team documentation update is deferred."""
import re
import unittest
from pathlib import Path
PROJECT=Path(__file__).resolve().parents[1]

class DocumentationContractTests(unittest.TestCase):
    def test_reference_proposals_and_team_guides_remain_available(self):
        for p in ("database.md","kafka-message-formats.md","README.md","KAFKA_FLINK_FLOW.md",
                  "docs/DATABASE_DESIGN.md","docs/ALERT_RULES_AND_FALSE_ALARM_MANAGEMENT.md"):
            self.assertTrue((PROJECT/p).is_file())

    def test_new_business_topics_match_supplied_proposal(self):
        from contracts import BUSINESS_TOPICS,DLQ_TOPIC
        text=(PROJECT/"kafka-message-formats.md").read_text(encoding="utf-8")
        for topic in (*BUSINESS_TOPICS.values(),DLQ_TOPIC): self.assertIn("`"+topic+"`",text)

    def test_target_business_columns_and_hypertables_match_proposal(self):
        from db import TABLE_COLUMNS
        text=(PROJECT/"database.md").read_text(encoding="utf-8")
        for table,columns in TABLE_COLUMNS.items():
            ddl=re.search(r"CREATE TABLE IF NOT EXISTS "+table+r" \((.*?)\);",text,re.S).group(1)
            for field in columns: self.assertRegex(ddl,r"\b"+field+r"\b")
        sql=(PROJECT/"docker/timescaledb/init/002_hypertables.sql").read_text()
        self.assertEqual(sql.count("create_hypertable"),4)
        self.assertNotIn("'chartevents'",sql)
        self.assertNotIn("'labevents'",sql)
