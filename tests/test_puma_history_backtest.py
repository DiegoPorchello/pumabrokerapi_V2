import unittest
from puma_history_backtest import normalize_history,contiguous_tail

def payload(times):
    return {"s":"ok","t":times,"o":[100]*len(times),"h":[101]*len(times),
            "l":[99]*len(times),"c":[100]*len(times)}
class TestHistory(unittest.TestCase):
    def test_sorts_and_deduplicates(self):
        self.assertEqual([c.time for c in normalize_history(payload([120,0,60,60]))],[0,60,120])
    def test_rejects_mismatched_columns(self):
        p=payload([0,60]);p["h"]=[101]
        with self.assertRaises(ValueError):normalize_history(p)
    def test_rejects_missing_history(self):
        with self.assertRaises(ValueError):normalize_history({"s":"no_data"})
    def test_contiguous_only(self):
        c=normalize_history(payload([0,60,120,300,360,420]))
        self.assertEqual([b.time for b in contiguous_tail(c,60,100)],[300,360,420])
    def test_1000_records(self):
        c=normalize_history(payload([i*60 for i in range(1100)]))
        self.assertEqual(len(contiguous_tail(c,60,1000)),1000)

    def test_login_returns_jwt_without_persisting(self):
        from puma_history_backtest import login_for_jwt
        from unittest.mock import Mock
        response=Mock()
        response.json.return_value={"accessToken":"example-jwt"}
        client=Mock()
        client.post.return_value=response
        self.assertEqual(login_for_jwt("user@example.com","secret",client),"example-jwt")
        self.assertEqual(client.post.call_args.kwargs["json"]["email"],"user@example.com")
    def test_login_rejects_missing_token(self):
        from puma_history_backtest import login_for_jwt
        from unittest.mock import Mock
        client=Mock()
        client.post.return_value.json.return_value={}
        with self.assertRaises(ValueError):
            login_for_jwt("user@example.com","secret",client)
if __name__=="__main__":
    unittest.main()
