import json

a =  b'{"prop": "45","iteration": 4,"thing_id": "aa-badformat"}'
payloadstr = a.decode('utf8').replace("'", '"')
payload = json.loads(payloadstr)

print(payload['thing_id'])