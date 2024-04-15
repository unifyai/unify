import requests

url = "https://api.unify.ai/v0/chat/completions"
headers = {
    "Authorization": "Bearer ZEAJnQYy69qXNsRpVP7t5CbxBMTeYgMP261FmCBOcfo=",
}

payload = {
    "model": "llama-2-7b-chat@lowest-itl",
    "messages": [{
        "role": "user",
        "content": "Hello!"
    }],
}

response = requests.post(url, json=payload, headers=headers)
if response.status_code == 200:
    print("success")
else:
    print("failed!")