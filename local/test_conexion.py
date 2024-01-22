import requests
response = requests.get("hhttps://bitbucket.telecom.com.ar/projects/BIAN/repos/cam-cdp/raw/DATAFEED/IND_MORA.sql?token=Mzc4ODU3NDgwMTE4OpHnX9alntVrDu465ZuAGw4prbHg")
sql_txt = response.text()