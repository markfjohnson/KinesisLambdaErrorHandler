import base64
#str = "{'thing_id': 'aa-OK', 'iteration':1, 'value':'something'}"
#str = "{'thing_id': 'aa-retry', 'iteration':1, 'value':'something'}"
str = "{'thing_id': 'aa-infra', 'iteration':3, 'value':'bad permissions'}"
str = "{'thing_id': 'aa-XXXX, 'iteration':3, 'value':'unknown'}"
str = "[{'thing_id': 'aa-XXXX, 'iteration':3, 'value':'unknown'},{'thing_id': 'aa-OK, 'iteration':3, 'value':'unknown'}]"
str_bytes = str.encode('utf-8')
payload_byte = base64.b64encode(str_bytes)
print(payload_byte)
payloadstr = base64.b64decode(payload_byte)
print(payloadstr)