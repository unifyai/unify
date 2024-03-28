import requests

url = "https://api.unify.ai/v0/chat/completions"
headers = { 
    "Authorization": "Bearer 3OYvVKJnmu8Z6DJzys9SInKe7P5mh9FPcNjstHLkiEw=",
}

payload = {
    "model": "llama-2-70b-chat@dog",
    "messages": [
        {
            "role": "user",
            "content": "Explain who Newton was and his entire theory of gravitation. Give a long detailed response please and explain all of his achievements"
        }],
    "stream": True
}

response = requests.post(url, json=payload, headers=headers, stream=True)

print(response.status_code)

if response.status_code == 200:
    for chunk in response.iter_content(chunk_size=1024):
        if chunk:
            print(chunk.decode("utf-8"))
else:
    print(response.text)