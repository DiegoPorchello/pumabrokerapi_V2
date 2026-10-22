import tempfile
import unittest
from pathlib import Path
from puma_hybrid_collector import HybridCollector

def event(kind,time,close=101):
    return {"type":kind,"symbol":"BETHUSDT","interval":"1",
            "bar":{"time":time,"open":100,"high":102,"low":99,"close":close}}

class TestHybridCollector(unittest.TestCase):
    def test_closes_and_deduplicates(self):
        with tempfile.TemporaryDirectory() as d:
            file=Path(d)/"bars.csv"
            c=HybridCollector(output=str(file))
            c.ingest(event("bar_update",60))
            self.assertFalse(file.exists())
            c.ingest(event("bar_closed",60))
            c.ingest(event("bar_closed",60))
            self.assertEqual(len(file.read_text().splitlines()),2)
    def test_does_not_trade_and_resets_gap(self):
        with tempfile.TemporaryDirectory() as d:
            c=HybridCollector(output=str(Path(d)/"bars.csv"))
            c.ingest(event("bar_closed",60))
            c.ingest(event("bar_closed",180))
            self.assertEqual(len(c.history),1)
            self.assertEqual(c.last_time,180)
if __name__=="__main__":
    unittest.main()
