import csv
import tempfile
import unittest
from pathlib import Path

from paper_probability_bot import Candle, backtest, read_csv, signal

class PaperBotTests(unittest.TestCase):
    def candles(self, n=120):
        return [Candle(i * 60, 100+i*.1, 101+i*.1, 99+i*.1, 100+i*.1) for i in range(n)]

    def test_validates_bars(self):
        with self.assertRaises(ValueError):
            Candle(0, 5, 4, 3, 5).validate()

    def test_rejects_duplicate_timestamps(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "bars.csv"
            with path.open("w", newline="") as fh:
                writer = csv.writer(fh)
                writer.writerow(["time","open","high","low","close"])
                writer.writerows([[60,1,2,1,2],[60,1,2,1,2]])
            with self.assertRaises(ValueError):
                read_csv(str(path))

    def test_flat_market_has_no_signals(self):
        bars = [Candle(i*60, 100, 100, 100, 100) for i in range(100)]
        report = backtest(bars)
        self.assertEqual(report["count"], 0)
        self.assertIsNone(report["win_rate"])

    def test_does_not_use_future_candles(self):
        bars = self.candles()
        original = signal(bars[:50])
        future = bars[:50] + [Candle(999999, 500, 600, 400, 550)]
        self.assertEqual(original, signal(future[:50]))

    def test_breakeven_and_invalid_params(self):
        report = backtest(self.candles(), payout=.85)
        self.assertAlmostEqual(report["breakeven_rate"], 1/1.85)
        with self.assertRaises(ValueError):
            backtest(self.candles(), expiry=0)
        with self.assertRaises(ValueError):
            backtest(self.candles(), payout=1.5)

    def test_no_overlapping_positions(self):
        report = backtest(self.candles(), expiry=3)
        for prev, current in zip(report["trades"], report["trades"][1:]):
            self.assertLess(prev["exit_time"], current["entry_time"])

if __name__ == "__main__":
    unittest.main()
