# Databricks notebook source
# MAGIC %md
# MAGIC # Sanjeevani-RAG: Step 2 - RAG Engine + FAISS Index + Outbreak Detection
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Builds FAISS vector index from clinical protocols
# MAGIC 2. Runs DBSCAN outbreak detection on triage logs
# MAGIC 3. Creates the RAG query function used by the Streamlit app

# COMMAND ----------

# MAGIC %pip install faiss-cpu sentence-transformers scikit-learn
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Build FAISS Vector Index from Clinical Protocols

# COMMAND ----------

import numpy as np
import faiss
import pickle
from sentence_transformers import SentenceTransformer

# Load clinical protocols from Delta table
df_protocols = spark.table("vc.sanjeevani.clinical_protocols").toPandas()

print(f"Loading {len(df_protocols)} protocol chunks...")

# Generate embeddings using a lightweight model
model = SentenceTransformer('all-MiniLM-L6-v2')
texts = df_protocols['chunk_text'].tolist()
embeddings = model.encode(texts, show_progress_bar=True, normalize_embeddings=True)

print(f"Embeddings shape: {embeddings.shape}")

# Build FAISS index (L2 distance on normalized vectors = cosine similarity)
dimension = embeddings.shape[1]
index = faiss.IndexFlatIP(dimension)  # Inner product on normalized = cosine
index.add(embeddings.astype('float32'))

print(f"FAISS index built with {index.ntotal} vectors")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test the FAISS Index

# COMMAND ----------

def search_protocols(query, top_k=3):
    """Search clinical protocols using FAISS"""
    query_vec = model.encode([query], normalize_embeddings=True).astype('float32')
    scores, indices = index.search(query_vec, top_k)
    results = []
    for i, (score, idx) in enumerate(zip(scores[0], indices[0])):
        row = df_protocols.iloc[idx]
        results.append({
            "rank": i+1,
            "score": float(score),
            "disease": row['disease_category'],
            "source": row['source_document'],
            "text": row['chunk_text']
        })
    return results

# Test queries
test_queries = [
    "patient bitten by snake swelling bleeding gums",
    "high fever joint pain rash vomiting 3 days",
    "child rapid breathing chest indrawing fever",
    "pregnant woman convulsions high blood pressure",
    "watery diarrhea severe dehydration child",
]

for q in test_queries:
    print(f"\n{'='*80}")
    print(f"QUERY: {q}")
    results = search_protocols(q)
    for r in results:
        print(f"  [{r['rank']}] Score: {r['score']:.3f} | Disease: {r['disease']}")
        print(f"      {r['text'][:120]}...")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save FAISS Index to Unity Catalog Volume

# COMMAND ----------

# Save FAISS index and metadata for the Streamlit app
import json
import os

# Create a temporary directory to save artifacts
save_dir = "/tmp/sanjeevani_faiss"
os.makedirs(save_dir, exist_ok=True)

# Save FAISS index
faiss.write_index(index, f"{save_dir}/clinical_protocols.index")

# Save metadata (protocol texts + disease categories)
metadata = {
    "texts": texts,
    "diseases": df_protocols['disease_category'].tolist(),
    "sources": df_protocols['source_document'].tolist(),
    "chunk_ids": df_protocols['chunk_id'].tolist(),
}
with open(f"{save_dir}/metadata.json", "w") as f:
    json.dump(metadata, f)

print("FAISS index and metadata saved to /tmp/sanjeevani_faiss/")
print(f"  - clinical_protocols.index ({os.path.getsize(f'{save_dir}/clinical_protocols.index')} bytes)")
print(f"  - metadata.json ({os.path.getsize(f'{save_dir}/metadata.json')} bytes)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Outbreak Detection using DBSCAN

# COMMAND ----------

from sklearn.cluster import DBSCAN
from datetime import datetime, timedelta
import math

# Load recent triage logs (last 48 hours)
df_triage = spark.table("vc.sanjeevani.triage_logs").toPandas()
df_triage['timestamp'] = pd.to_datetime(df_triage['timestamp'])

import pandas as pd
cutoff = datetime.now() - timedelta(hours=48)
df_recent = df_triage[df_triage['timestamp'] >= cutoff].copy()
print(f"Triage logs in last 48 hours: {len(df_recent)}")

# COMMAND ----------

# Run DBSCAN for each symptom cluster
def detect_outbreaks(df, eps_km=10, min_samples=3):
    """
    Run DBSCAN geospatial clustering on triage logs.
    eps_km: cluster radius in km
    min_samples: minimum cases to form a cluster
    """
    # Convert km to approximate degrees (at Indian latitudes ~22N)
    # 1 degree lat = ~111km, 1 degree lng = ~102km at 22N
    eps_degrees = eps_km / 111.0
    
    alerts = []
    
    for cluster_name in df['symptom_cluster'].unique():
        df_cluster = df[df['symptom_cluster'] == cluster_name]
        
        if len(df_cluster) < min_samples:
            continue
        
        coords = df_cluster[['latitude', 'longitude']].values
        
        # DBSCAN with haversine-approximate distance
        clustering = DBSCAN(eps=eps_degrees, min_samples=min_samples).fit(coords)
        
        labels = clustering.labels_
        n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
        
        if n_clusters > 0:
            for cluster_id in range(n_clusters):
                mask = labels == cluster_id
                cluster_points = coords[mask]
                cluster_cases = df_cluster[mask]
                
                centroid_lat = cluster_points[:, 0].mean()
                centroid_lng = cluster_points[:, 1].mean()
                case_count = len(cluster_points)
                
                # Calculate approximate radius
                max_dist = 0
                for p in cluster_points:
                    d = math.sqrt((p[0] - centroid_lat)**2 + (p[1] - centroid_lng)**2) * 111
                    max_dist = max(max_dist, d)
                
                # Severity based on case count
                if case_count >= 10:
                    severity = "RED"
                elif case_count >= 5:
                    severity = "ORANGE"
                else:
                    severity = "YELLOW"
                
                disease = {
                    "dengue_like": "Dengue Fever",
                    "malaria_like": "Malaria",
                    "gastro_acute": "Acute Gastroenteritis / Cholera",
                    "snakebite": "Snakebite Cluster",
                    "ari_pneumonia": "Respiratory Infection",
                    "maternal_emergency": "Maternal Emergency"
                }.get(cluster_name, cluster_name)
                
                districts = ", ".join(cluster_cases['district'].unique())
                
                alerts.append({
                    "alert_id": f"ALERT-{cluster_name}-{cluster_id+1}",
                    "detected_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "disease_suspected": disease,
                    "symptom_cluster": cluster_name,
                    "centroid_lat": round(centroid_lat, 6),
                    "centroid_lng": round(centroid_lng, 6),
                    "radius_km": round(max_dist, 2),
                    "case_count": case_count,
                    "severity": severity,
                    "affected_districts": districts,
                    "status": "ACTIVE"
                })
                
                print(f"OUTBREAK DETECTED: {disease}")
                print(f"  Location: ({centroid_lat:.4f}, {centroid_lng:.4f})")
                print(f"  Cases: {case_count} | Severity: {severity}")
                print(f"  Radius: {max_dist:.1f} km | Districts: {districts}")
                print()
    
    return alerts

alerts = detect_outbreaks(df_recent, eps_km=10, min_samples=3)
print(f"\nTotal outbreak alerts: {len(alerts)}")

# COMMAND ----------

# Save outbreak alerts to Delta table
if alerts:
    from pyspark.sql import Row
    alert_rows = [Row(**a) for a in alerts]
    df_alerts = spark.createDataFrame(alert_rows)
    df_alerts.write.mode("overwrite").saveAsTable("vc.sanjeevani.outbreak_alerts")
    print(f"Saved {len(alerts)} outbreak alerts to vc.sanjeevani.outbreak_alerts")
    display(df_alerts)
else:
    print("No outbreaks detected")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Test the Full RAG Pipeline (LLM Integration)

# COMMAND ----------

# Test calling Foundation Model API
from openai import OpenAI
import os

# When running in Databricks notebook, we can use the built-in token
# Get workspace URL from spark config
workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

client = OpenAI(
    base_url=f"https://{workspace_url}/serving-endpoints",
    api_key=token,
)

# Test with a simple query to check which models are available
try:
    response = client.chat.completions.create(
        model="databricks-meta-llama-3-3-70b-instruct",
        messages=[
            {"role": "system", "content": "You are a rural healthcare triage assistant for India. Reply in clear, actionable bullet points."},
            {"role": "user", "content": "Patient has high fever for 3 days with joint pain and rash. What could it be and what should the ASHA worker do?"}
        ],
        max_tokens=500,
        temperature=0.3,
    )
    print("Foundation Model API works!")
    print(response.choices[0].message.content)
except Exception as e:
    print(f"Foundation Model API error: {e}")
    print("\nTrying alternative model name...")
    try:
        response = client.chat.completions.create(
            model="databricks-dbrx-instruct",
            messages=[
                {"role": "system", "content": "You are a rural healthcare triage assistant."},
                {"role": "user", "content": "Patient has high fever for 3 days with joint pain and rash."}
            ],
            max_tokens=500,
        )
        print("DBRX works!")
        print(response.choices[0].message.content)
    except Exception as e2:
        print(f"DBRX also failed: {e2}")
        print("\nWill need to use external LLM API (Groq/Gemini). See next cell.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Full RAG Query Function

# COMMAND ----------

def sanjeevani_rag_query(symptoms_text, patient_lat, patient_lng, patient_age=None, patient_gender=None):
    """
    Full RAG pipeline:
    1. Search clinical protocols (FAISS)
    2. Check drug inventory at nearby facilities (Delta Table)
    3. Generate AI response with routing
    """
    
    # Step 1: Clinical RAG
    rag_results = search_protocols(symptoms_text, top_k=3)
    protocol_context = "\n\n".join([f"[Protocol - {r['disease']}]: {r['text']}" for r in rag_results])
    top_disease = rag_results[0]['disease'] if rag_results else "Unknown"
    
    # Step 2: Find required drugs based on suspected disease
    disease_drug_map = {
        "Snakebite": ["Polyvalent Anti-Snake Venom (ASV)", "Normal Saline 0.9% (1L)", "Adrenaline (Epinephrine) 1mg/ml", "Hydrocortisone Injection 100mg"],
        "Dengue Fever": ["Dengue NS1 Antigen Test Kit", "Normal Saline 0.9% (1L)", "Ringer Lactate (1L)", "Paracetamol 500mg"],
        "Malaria": ["Artesunate Injection 60mg", "Artemether-Lumefantrine Tablets", "Chloroquine Phosphate 250mg"],
        "Acute Gastroenteritis": ["ORS Packets (WHO formula)", "Zinc Sulphate Dispersible 20mg", "Normal Saline 0.9% (1L)", "Ringer Lactate (1L)"],
        "Pneumonia": ["Amoxicillin 500mg", "Paracetamol 500mg", "Salbutamol Inhaler 100mcg"],
        "Obstetric Emergency": ["Oxytocin Injection 5IU", "Misoprostol 200mcg", "Magnesium Sulphate Injection 50%", "Normal Saline 0.9% (1L)"],
        "Rabies": ["Anti-Rabies Vaccine (ARV)", "Rabies Immunoglobulin (RIG)", "Tetanus Toxoid Injection"],
    }
    
    required_drugs = disease_drug_map.get(top_disease, [])
    
    # Step 3: Check inventory at nearby facilities
    # Simple haversine approximation (good enough for ~100km)
    df_inv = spark.sql(f"""
        SELECT f.facility_id, f.facility_name, f.facility_type, f.district,
               f.latitude, f.longitude, f.beds,
               i.drug_name, i.stock_status, i.quantity_available,
               SQRT(POW((f.latitude - {patient_lat}) * 111, 2) + POW((f.longitude - {patient_lng}) * 102, 2)) as distance_km
        FROM vc.sanjeevani.health_facilities f
        JOIN vc.sanjeevani.drug_inventory i ON f.facility_id = i.facility_id
        WHERE i.drug_name IN ({','.join([f"'{d}'" for d in required_drugs])})
        ORDER BY distance_km
    """).toPandas()
    
    # Find best facility (has all required drugs in stock, nearest)
    facility_scores = {}
    for _, row in df_inv.iterrows():
        fid = row['facility_id']
        if fid not in facility_scores:
            facility_scores[fid] = {
                "name": row['facility_name'],
                "type": row['facility_type'],
                "district": row['district'],
                "distance_km": row['distance_km'],
                "drugs_available": [],
                "drugs_missing": [],
            }
        if row['stock_status'] == "IN_STOCK":
            facility_scores[fid]["drugs_available"].append(row['drug_name'])
        else:
            facility_scores[fid]["drugs_missing"].append(f"{row['drug_name']} ({row['stock_status']})")
    
    # Sort by: has all drugs > nearest
    ranked_facilities = sorted(facility_scores.values(), key=lambda x: (len(x['drugs_missing']), x['distance_km']))
    
    routing_context = ""
    for i, f in enumerate(ranked_facilities[:5]):
        status = "READY" if not f['drugs_missing'] else "PARTIAL"
        routing_context += f"\n{i+1}. {f['name']} ({f['type']}) - {f['distance_km']:.1f}km - {status}"
        if f['drugs_missing']:
            routing_context += f"\n   MISSING: {', '.join(f['drugs_missing'])}"
        routing_context += f"\n   AVAILABLE: {', '.join(f['drugs_available'][:3])}..."
    
    # Step 4: Check for active outbreaks
    outbreak_context = ""
    try:
        df_outbreaks = spark.table("vc.sanjeevani.outbreak_alerts").filter("status = 'ACTIVE'").toPandas()
        if len(df_outbreaks) > 0:
            outbreak_context = "\n\nACTIVE OUTBREAK ALERTS:\n"
            for _, alert in df_outbreaks.iterrows():
                outbreak_context += f"- {alert['severity']} ALERT: {alert['disease_suspected']} ({alert['case_count']} cases) near ({alert['centroid_lat']}, {alert['centroid_lng']})\n"
    except:
        pass
    
    # Step 5: Generate LLM response
    system_prompt = """You are Sanjeevani, an AI-powered rural healthcare triage assistant for ASHA workers in Madhya Pradesh, India.

Your job is to:
1. Analyze symptoms and provide a likely diagnosis
2. Give clear, actionable first-aid/protocol steps from MOHFW guidelines
3. Route the patient to the nearest equipped facility
4. Flag any active outbreak alerts in the area

RULES:
- Be concise and use bullet points
- Mention specific drugs/doses from the protocols
- ALWAYS warn if the nearest facility is OUT OF STOCK
- Include severity level (MILD/MODERATE/SEVERE/CRITICAL)
- If related to a potential outbreak, flag it prominently
- Use simple language an ASHA worker can understand"""

    user_prompt = f"""SYMPTOMS REPORTED: {symptoms_text}
PATIENT LOCATION: ({patient_lat}, {patient_lng})
{"PATIENT AGE: " + str(patient_age) if patient_age else ""}
{"PATIENT GENDER: " + str(patient_gender) if patient_gender else ""}

MATCHING CLINICAL PROTOCOLS:
{protocol_context}

NEAREST FACILITY INVENTORY STATUS:
{routing_context}
{outbreak_context}

Provide your triage assessment, recommended protocol, and facility routing."""

    try:
        response = client.chat.completions.create(
            model="databricks-meta-llama-3-3-70b-instruct",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            max_tokens=800,
            temperature=0.3,
        )
        ai_response = response.choices[0].message.content
    except Exception as e:
        ai_response = f"[LLM unavailable: {str(e)}]\n\nBased on protocols:\n- Suspected: {top_disease}\n- Top protocol: {rag_results[0]['text'][:200] if rag_results else 'N/A'}\n\nNearest equipped facility: {ranked_facilities[0]['name'] if ranked_facilities else 'N/A'} ({ranked_facilities[0]['distance_km']:.1f}km)" if ranked_facilities else "[No facilities found]"
    
    return {
        "diagnosis": top_disease,
        "ai_response": ai_response,
        "rag_results": rag_results,
        "facilities": ranked_facilities[:5],
        "outbreaks": outbreak_context,
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test Full Pipeline

# COMMAND ----------

# Test 1: Snakebite near PHC that may be out of ASV
result = sanjeevani_rag_query(
    "saanp ne kaata, soojan aa rahi hai, masoodhe se khoon aa raha hai",
    patient_lat=21.82, patient_lng=76.35,
    patient_age=35, patient_gender="M"
)
print("DIAGNOSIS:", result['diagnosis'])
print("\nAI RESPONSE:")
print(result['ai_response'])
print("\nTOP FACILITIES:")
for f in result['facilities'][:3]:
    print(f"  - {f['name']} ({f['type']}, {f['distance_km']:.1f}km) Missing: {f['drugs_missing'] or 'None'}")

# COMMAND ----------

# Test 2: Dengue symptoms near the outbreak cluster
result2 = sanjeevani_rag_query(
    "teen din se tez bukhar, jodon me dard, shareer pe lal daane, ulti ho rahi hai",
    patient_lat=22.56, patient_lng=75.76,
    patient_age=28, patient_gender="F"
)
print("DIAGNOSIS:", result2['diagnosis'])
print("\nAI RESPONSE:")
print(result2['ai_response'])

# COMMAND ----------

# Test 3: Child pneumonia
result3 = sanjeevani_rag_query(
    "bacche ko khansi hai 5 din se, saans tez chal rahi hai, chhati dhans rahi hai, bukhar bhi hai",
    patient_lat=22.72, patient_lng=75.86,
    patient_age=3, patient_gender="M"
)
print("DIAGNOSIS:", result3['diagnosis'])
print("\nAI RESPONSE:")
print(result3['ai_response'])
