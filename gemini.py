import requests
import json

def gemini_generate_content(api_key, text):
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash-latest:generateContent?key={api_key}"
    data = {
        "contents": [{
            "parts": [{
                "text": text
            }]
        }]
    }
    response = requests.post(url, headers={"Content-Type": "application/json"}, json=data)
    if response.status_code == 200:
        return response.json()
    else:
        return None


