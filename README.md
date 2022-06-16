# ETDEX
All ETDEX programs, to track all landings using various sources.

## Requirements
At least python 3.8 (didn't test for version older than this, it could work).
To dockerize this application, just write `docker-compose up` in the terminal.

To all access to the different APIs, you will need different JSON-file:
For the AFTN-block, you will need a file called "auth_aftn.json", and the data in it should be like this:
```
{
  "username": <api_username>,
  "password": <api_password>
}
```
The PPR program needs a file called "auth_ppr.json", and all programs using the aeroPS AVDB need a file called "auth_avdb.json". Both have the same form:
```
{
  "user": <api_username>,
  "password": <api_password>
}
```
All get_airplane programs also need an auth_file, but all of them are regrouped in the same file "auth_api.json". Here is the form of this file:
```
{
  <source>: {
    "user": <api_username/type>
    "key": <api_key/api_password>
  },
  ...
}
```
Third-party module :
> requests (for making all API call)
> 
> jellyfish (for computing the correlation between two string)

