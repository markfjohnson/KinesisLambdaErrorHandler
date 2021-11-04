import base64
#str = "{'thing_id': 'aa-OK', 'iteration':1, 'value':'something'}"
str = "{'thing_id': 'aa-badformat', 'iteration':1, 'value':'something'}"
str_bytes = str.encode('utf-8')
payload_byte = base64.b64encode(str_bytes)
print(payload_byte)
payloadstr = base64.b64decode(payload_byte)
print(payloadstr)