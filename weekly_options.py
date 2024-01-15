import http.client

conn = http.client.HTTPConnection('192.168.50.41', 8080)
headers = {"Accept": "application/json"}
conn.request('POST', '/stocks/ticker/weeklyoptions', None, headers)
conn.set_debuglevel(4)
response = conn.getresponse()
print(response.status)
print(response.read())
