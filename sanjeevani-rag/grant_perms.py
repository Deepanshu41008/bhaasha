import requests

TOKEN = ""
HOST = "https://dbc-746e6173-db1c.cloud.databricks.com"
WH_ID = "1dabc4f0ba6299c7"

headers = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

# Try different principal names
principals = ["`account users`", "`Account Users`"]
grants = ["USE_CATALOG", "USE_SCHEMA", "SELECT"]

for principal in principals:
    sqls = [
        f"GRANT USE_CATALOG ON CATALOG vc TO {principal}",
        f"GRANT USE_SCHEMA ON SCHEMA vc.sanjeevani TO {principal}",
        f"GRANT SELECT ON SCHEMA vc.sanjeevani TO {principal}",
    ]
    print(f"\n=== Trying principal: {principal} ===")
    for sql in sqls:
        print(f"  {sql}")
        resp = requests.post(
            f"{HOST}/api/2.0/sql/statements/",
            headers=headers,
            json={"warehouse_id": WH_ID, "statement": sql, "wait_timeout": "30s"}
        )
        data = resp.json()
        state = data.get("status", {}).get("state", "UNKNOWN")
        if state == "SUCCEEDED":
            print(f"    -> SUCCESS!")
        else:
            err = data.get("status", {}).get("error", {}).get("message", "unknown")
            print(f"    -> {state}: {err[:100]}")
    # If first principal works, break
    if state == "SUCCEEDED":
        break

# Also try: all_principals to understand what exists
print("\n=== Listing service principals ===")
resp = requests.get(
    f"{HOST}/api/2.0/preview/scim/v2/ServicePrincipals",
    headers={"Authorization": f"Bearer {TOKEN}"}
)
for sp in resp.json().get("Resources", []):
    print(f"  SP: '{sp['displayName']}' (id={sp['id']}, appId={sp.get('applicationId','')})")

# Try with exact display name from SCIM
print("\n=== Trying with quoted displayName ===")
resp2 = requests.get(
    f"{HOST}/api/2.0/preview/scim/v2/ServicePrincipals",
    headers={"Authorization": f"Bearer {TOKEN}"}
)
for sp in resp2.json().get("Resources", []):
    if "sanjeevani" in sp['displayName']:
        dn = sp['displayName']
        app_id = sp.get('applicationId', '')
        print(f"  Found: '{dn}' appId={app_id}")
        # Try granting with applicationId
        for sql_tmpl in [
            f"GRANT ALL PRIVILEGES ON CATALOG vc TO `{app_id}`",
            f"GRANT ALL PRIVILEGES ON SCHEMA vc.sanjeevani TO `{app_id}`",
        ]:
            print(f"  Running: {sql_tmpl}")
            r = requests.post(
                f"{HOST}/api/2.0/sql/statements/",
                headers=headers,
                json={"warehouse_id": WH_ID, "statement": sql_tmpl, "wait_timeout": "30s"}
            )
            d = r.json()
            s = d.get("status", {}).get("state", "?")
            if s == "SUCCEEDED":
                print(f"    -> SUCCESS!")
            else:
                print(f"    -> {s}: {d.get('status',{}).get('error',{}).get('message','')[:120]}")
