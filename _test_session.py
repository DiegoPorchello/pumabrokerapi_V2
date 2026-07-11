import requests

base = "https://trade.pumabroker.com"
h = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0.0.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
}

# Step 1: Visit main page to get Cloudflare/session cookies
s = requests.Session()
s.headers.update(h)
r1 = s.get(base + "/", timeout=15)
print(f"GET / status: {r1.status_code}")
print(f"Cookies: {dict(s.cookies)}")
print(f"Headers: {dict(r1.headers)}")
print()

# Step 2: Now try login with the session cookies
login_headers = {
    "Content-Type": "application/json",
    "Accept": "application/json, text/plain, */*",
    "Origin": base,
    "Referer": base + "/",
    "X-Requested-With": "XMLHttpRequest",
}

payload = {"email": "test@test.com", "password": "test123"}
r2 = s.post(base + "/api/v1/auth/login", json=payload, headers=login_headers, timeout=10)
print(f"POST login status: {r2.status_code}")
print(f"Response: {r2.text[:300]}")
print(f"Cookies after: {dict(s.cookies)}")
