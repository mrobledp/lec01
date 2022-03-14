import requests

url = "http://172.17.0.1/modeling"

payload={}
headers = {
  'Authorization': 'Basic bW9kZWxlcjpwYXNzd29yZA=='
}

response = requests.request("GET", url, headers=headers, data=payload)

print(response.text)

