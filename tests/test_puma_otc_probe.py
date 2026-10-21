import unittest
from puma_otc_probe import describe_event

class TestOtcProbe(unittest.TestCase):
    def test_detects_nested_target_without_exposing_data(self):
        payload = {"data":{"symbol":"BETH/USDT","close":740.1,"token":"do-not-log"}}
        result=describe_event("candle", payload)
        self.assertTrue(result["target_present"])
        self.assertNotIn("do-not-log",str(result))
        self.assertNotIn("740.1",str(result))
    def test_does_not_match_wrong_asset(self):
        self.assertFalse(describe_event("tick",{"symbol":"EURUSD"})["target_present"])
if __name__=="__main__":
    unittest.main()
