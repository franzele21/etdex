import requests
import json

AUTH_FILE = "auth.json"
OUTPUT_FILE = "access_token.json"

with open(AUTH_FILE) as file:
	payload = file.read()

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
