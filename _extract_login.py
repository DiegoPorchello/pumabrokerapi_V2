import requests, re, json

r = requests.get("https://trade.pumabroker.com/assets/index-BeTtYIY4.js", timeout=30)
text = r.text

# Find axios instance configuration (baseURL, headers, etc.)
for m in re.finditer(r'baseURL[=:][^,;\n]+', text):
    ctx = text[max(0,m.start()-50):m.end()+50]
    print("AXIOS BASE:", ctx[:150])
    print("---")

# Find the login function/method
for m in re.finditer(r'login[a-zA-Z]*\s*[=:]\s*(?:function|async)?[^{]*\{[^}]{50,500}\}', text):
    snippet = m.group()[:400]
    print("LOGIN FN:", snippet)
    print("---")

# Find all axios/fetch calls with "auth/login" or "/login" in context
for m in re.finditer(r'["\'][^"\']*login[^"\']*["\']', text):
    start = max(0, m.start()-200)
    end = min(len(text), m.end()+200)
    ctx = text[start:end]
    # Only print if it looks like an API call (contains axios, fetch, post, etc.)
    if any(x in ctx.lower() for x in ["axios", "fetch", ".post", ".get", "request"]):
        print("API CALL:", ctx[:300])
        print("===")
