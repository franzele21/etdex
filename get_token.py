import requests
import json
import sys

AUTH_FILE = "auth.json"
OUTPUT_FILE = "access_token.json"

try:
	with open(AUTH_FILE) as file:
		payload = file.read()
except FileNotFoundError:
	print(f"""Error: the {AUTH_FILE} don't exists. 
In this file, you need to have the data ordered like this:
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

with open(OUTPUT_FILE, "w+") as output_file:
	output_file.write(json.dumps(
				{
					"access_token": access_token,
					"expiration_date": expiration_date,
					"token_type": "bearer"
				}, indent=2))
