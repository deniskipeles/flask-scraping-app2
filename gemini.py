import requests
import json

def get_text(json_data):
    text = None
    try:
        text = json_data["candidates"][0]["content"]["parts"][0]["text"]
    except (KeyError, IndexError):
        pass
    return text
    
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
        json_data = response.json()
        return get_text(json_data)
    else:
        return None


