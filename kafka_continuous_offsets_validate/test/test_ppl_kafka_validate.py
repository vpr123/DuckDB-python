import unittest

import duckdb
from helpers import runQuery, say
from pipeline.ppl_kafka_validate import run_transform

class SparkETLTests(unittest.TestCase):

    def setUp(self):
        runQuery("CREATE TEMP TABLE kafka_messages AS SELECT * FROM read_csv('test/data/kafka_messages.csv')")

    def tearDown(self):
        duckdb.close()
        print("\n")
        say("DuckDB connection closed")

    def test_transform(self):

        res = run_transform('2024-12-28', '10')
        print("\n")

        runQuery("select * from t_stg_events").show()
        runQuery("select * from t_stg_event_continuous_ranges").show()

        say("RESULT")
        res.show()

        print("\n\n ========= Unit Tests ========= \n\n")

        expected_row_count = 3
        self.assertEqual(len(res), expected_row_count)
        say("Final dataset row count = " + str(expected_row_count) + " validation passed");

if __name__ == '__main__':
    unittest.main()