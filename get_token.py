"""
This programm ask the AFTN API a (new) token, so the get_aftn_by_id.py
can make its calls
"""

import requests
import json
import sys

AUTH_FILE = "auth_aftn.json"
OUTPUT_FILE = "access_token.json"

try:
	with open(AUTH_FILE) as file:
		payload = file.read()
except FileNotFoundError:
	# if the file isn't available, it will display an error message 
	# with an explanation of the format the file 
	print(f"""Error: the {AUTH_FILE} doesn't exist. 
In this file, you need to have the data like this:
{{
	"username": "<auth_username>",
	"password": "<auth_password>"
}}
""")
	sys.exit(1)

response = requests.post(" https://aftn.pno.cloud/aftnmailbox/token/generate.php",
                        data=payload)

response = json.loads(response.text)
access_token = response["document"]["access_token"]
expiration_date = response["document"]["expires_in"]

# the written data are: the access token, its expiration date, and the
# method of request
with open(OUTPUT_FILE, "w+") as output_file:
	output_file.write(json.dumps(
				{
					"access_token": access_token,
					"expiration_date": expiration_date,
					"token_type": "bearer"
				}, indent=2))
