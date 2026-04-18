import requests

TOKEN = ""
HOST = "https://dbc-746e6173-db1c.cloud.databricks.com"
APP_ID = "a9a4f815-be2e-47ca-9d59-30b2d70bf5dc"
SP_ID = "71144695649743"
SP_NAME = "app-2e2den sanjeevani-rag"

endpoints = [
    "databricks-meta-llama-3-3-70b-instruct",
    "databricks-bge-large-en"
]

headers = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

for ep in endpoints:
    url = f"{HOST}/api/2.0/permissions/serving-endpoints/{ep}"
    
    # Try SP Name
    payload = {
        "access_control_list": [
            {
                "service_principal_name": SP_NAME,
                "permission_level": "CAN_QUERY"
            }
        ]
    }
    
    print(f"Granting CAN_QUERY on {ep}")
    resp = requests.patch(url, headers=headers, json=payload)
    print("PATCH result:", resp.status_code, resp.text)
