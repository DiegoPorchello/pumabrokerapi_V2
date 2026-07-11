import requests, json

base = "https://trade.pumabroker.com"
h = {
    "Content-Type": "application/json",
    "Accept": "application/json, text/plain, */*",
    "Origin": base,
    "Referer": base + "/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0.0.0",
}

payloads = [
    ({"email": "test@test.com", "password": "test123"}, "email+password"),
    ({"email": "test@test.com", "password": "test123", "rememberMe": True}, "with rememberMe"),
    ({"email": "test@test.com", "password": "test123", "remember": True}, "with remember"),
    ({"username": "test@test.com", "password": "test123"}, "username+password"),
    ({"login": "test@test.com", "password": "test123"}, "login+password"),
]

for payload, label in payloads:
    r = requests.post(base + "/api/v1/auth/login", json=payload, headers=h, timeout=10)
    body = r.text[:200].replace("\n", " ").replace("\r", "")
    print(f"{r.status_code:3d} {label:30s} {body}")
