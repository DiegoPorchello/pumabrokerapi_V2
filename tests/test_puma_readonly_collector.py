import tempfile
import unittest
from pathlib import Path
from paper_probability_bot import signal
from puma_readonly_collector import ClosedBarCollector

def bar(t, close=100):
    return {"type":"bar_update","symbol":"BETHUSDT","interval":"1","bar":{
        "time":t,"open":100,"high":max(100,close)+1,"low":min(100,close)-1,"close":close}}

class TestCollector(unittest.TestCase):
    def test_only_closes_after_next_bar(self):
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/"candles.csv"
            c=ClosedBarCollector(output=path)
            c.ingest(bar(0))
            c.ingest(bar(0,101))
            self.assertFalse(path.exists())
            c.ingest(bar(60))
            self.assertEqual(len(c.history),1)
            self.assertEqual(c.history[0].close,101)
            self.assertEqual(len(path.read_text().splitlines()),2)

    def test_gap_discards_unreliable_chain(self):
        with tempfile.TemporaryDirectory() as folder:
            c=ClosedBarCollector(output=str(Path(folder)/"candles.csv"))
            c.ingest(bar(0)); c.ingest(bar(60)); c.ingest(bar(180))
            self.assertEqual(len(c.history),0)
            self.assertEqual(c.gap_count,1)

    def test_wrong_symbol_old_and_bad_bars(self):
        with tempfile.TemporaryDirectory() as folder:
            c=ClosedBarCollector(output=str(Path(folder)/"candles.csv"))
            wrong=bar(0); wrong["symbol"]="EURUSD"
            c.ingest(wrong)
            self.assertIsNone(c.current)
            c.ingest(bar(60))
            c.ingest(bar(0))
            self.assertEqual(c.current.time,60)
            bad=bar(120); bad["bar"]["high"]=10
            c.ingest(bad)
            self.assertEqual(c.current.time,60)

if __name__=="__main__":
    unittest.main()
