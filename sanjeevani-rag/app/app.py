import streamlit as st
import json
import math
import os
import numpy as np
import pandas as pd
from datetime import datetime

# ─── Page Config ───
st.set_page_config(
    page_title="BHAASHA - Population Health Router",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ─── Custom CSS ───
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
* { font-family: 'Inter', sans-serif; }
.stApp { background: linear-gradient(135deg, #0f172a 0%, #1e293b 50%, #0f172a 100%); }
.card {
    background: rgba(30, 41, 59, 0.8);
    border: 1px solid rgba(148, 163, 184, 0.1);
    border-radius: 12px; padding: 20px;
    backdrop-filter: blur(10px); margin-bottom: 12px;
}
.alert-red { border-left: 4px solid #ef4444; background: rgba(239,68,68,0.1); padding: 15px; border-radius: 8px; margin: 10px 0; }
.alert-orange { border-left: 4px solid #f97316; background: rgba(249,115,22,0.1); padding: 15px; border-radius: 8px; margin: 10px 0; }
.alert-yellow { border-left: 4px solid #eab308; background: rgba(234,179,8,0.1); padding: 15px; border-radius: 8px; margin: 10px 0; }
</style>
""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════
# AUTHENTICATION HELPER - Works with Databricks Apps
# Service principals use OAuth, not PAT tokens
# ═══════════════════════════════════════════════════════

def get_databricks_auth():
    """
    Get Databricks host and bearer token.
    Databricks Apps use service principal OAuth - we get the token
    via w.config.authenticate() which returns auth headers.
    """
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        host = w.config.host

        # Extract bearer token — multiple approaches for different SDK versions
        token = None

        # Method 1: Direct token property (PAT-based auth)
        try:
            t = w.config.token
            if t:
                token = t
        except Exception:
            pass

        # Method 2: Header factory — authenticate is a callable that returns headers
        if not token:
            try:
                header_factory = w.config.authenticate
                if callable(header_factory):
                    result = header_factory()
                    if isinstance(result, dict):
                        auth_val = result.get("Authorization", "")
                        token = auth_val.replace("Bearer ", "") if auth_val else None
            except Exception:
                pass

        # Method 3: Internal OAuth token source
        if not token:
            try:
                token = w.config._token_source.token().access_token
            except Exception:
                pass

        # Method 4: Get token via a simple API call's auth headers
        if not token:
            try:
                import requests as _req
                session = _req.Session()
                w.config.authenticate(session)
                auth_val = session.headers.get("Authorization", "")
                token = auth_val.replace("Bearer ", "") if auth_val else None
            except Exception:
                pass

        return w, host, token
    except Exception as e:
        st.sidebar.error(f"Databricks auth error: {e}")
        return None, None, None


# ═══════════════════════════════════════════════════════
# SQL QUERY HELPER - Uses Statement Execution API
# ═══════════════════════════════════════════════════════

@st.cache_resource
def get_warehouse_id():
    """Find a SQL warehouse to use for queries."""
    return os.environ.get("DATABRICKS_WAREHOUSE_ID", "1dabc4f0ba6299c7")


def run_sql(query):
    """Execute SQL and return pandas DataFrame."""
    w, _, _ = get_databricks_auth()
    wh_id = get_warehouse_id()
    if w is None or wh_id is None:
        return pd.DataFrame()
    try:
        from databricks.sdk.service.sql import StatementState
        result = w.statement_execution.execute_statement(
            warehouse_id=wh_id,
            statement=query,
            wait_timeout="50s"
        )
        if result.status:
            if result.status.state == StatementState.SUCCEEDED:
                if result.manifest and result.result and result.result.data_array:
                    columns = [col.name for col in result.manifest.schema.columns]
                    return pd.DataFrame(result.result.data_array, columns=columns)
                return pd.DataFrame()  # DML success (INSERT/UPDATE) — no rows returned
            elif result.status.state == StatementState.FAILED:
                err_msg = result.status.error.message if result.status.error else "Unknown SQL error"
                st.error(f"SQL Failed: {err_msg}")
                return pd.DataFrame()
        return pd.DataFrame()
    except Exception as e:
        st.error(f"SQL Error: {e}")
        return pd.DataFrame()


# ═══════════════════════════════════════════════════════
# DATA LOADING (cached, via SQL)
# ═══════════════════════════════════════════════════════

@st.cache_data(ttl=300)
def load_facilities():
    df = run_sql("SELECT * FROM vc.sanjeevani.health_facilities")
    for c in ['latitude','longitude','beds']:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce')
    return df

@st.cache_data(ttl=300)
def load_inventory():
    df = run_sql("SELECT * FROM vc.sanjeevani.drug_inventory")
    if 'quantity_available' in df.columns:
        df['quantity_available'] = pd.to_numeric(df['quantity_available'], errors='coerce')
    return df

@st.cache_data(ttl=300)
def load_triage_logs():
    df = run_sql("SELECT * FROM vc.sanjeevani.triage_logs")
    for c in ['latitude','longitude','severity_score','patient_age']:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce')
    return df

@st.cache_data(ttl=300)
def load_outbreaks():
    df = run_sql("SELECT * FROM vc.sanjeevani.outbreak_alerts")
    for c in ['centroid_lat','centroid_lng','radius_km','case_count']:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce')
    return df

@st.cache_data(ttl=60)
def load_recent_triage_logs():
    """Fetch only the last 48 hours to speed up dynamic outbreak calculations."""
    df = run_sql("SELECT * FROM vc.sanjeevani.triage_logs WHERE timestamp >= current_timestamp() - INTERVAL 48 HOURS")
    for c in ['latitude','longitude','severity_score','patient_age']:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce')
    return df

@st.cache_data(ttl=600)
def load_protocols():
    return run_sql("SELECT * FROM vc.sanjeevani.clinical_protocols")


# ═══════════════════════════════════════════════════════
# FAISS INDEX (built from SQL-loaded data)
# ═══════════════════════════════════════════════════════

@st.cache_resource
def init_faiss_index():
    """Build FAISS vector index from clinical protocols loaded via SQL."""
    try:
        import faiss
        from sentence_transformers import SentenceTransformer

        df = load_protocols()
        if df.empty:
            return None, None, [], [], []

        texts = df['chunk_text'].tolist()
        diseases = df['disease_category'].tolist()
        sources = df['source_document'].tolist()

        client, _ = init_llm_client()
        if client:
            response = client.embeddings.create(
                model="databricks-bge-large-en",
                input=texts
            )
            embeddings = np.array([res.embedding for res in response.data]).astype('float32')
            model = client
        else:
            from sentence_transformers import SentenceTransformer
            model = SentenceTransformer('all-MiniLM-L6-v2')
            embeddings = model.encode(texts, normalize_embeddings=True).astype('float32')
            
        import faiss
        index = faiss.IndexFlatIP(embeddings.shape[1])
        index.add(embeddings)

        return model, index, texts, diseases, sources
    except Exception as e:
        st.sidebar.error(f"FAISS error: {e}")
        return None, None, [], [], []


# ═══════════════════════════════════════════════════════
# LLM CLIENT
# ═══════════════════════════════════════════════════════

@st.cache_resource
def init_llm_client():
    """Initialize LLM. Databricks Foundation Model > Groq > Gemini > None."""
    from openai import OpenAI

    # 1) Databricks Foundation Model API
    try:
        _, host, token = get_databricks_auth()
        if host and token:
            host_clean = host.replace("https://", "").replace("http://", "").rstrip("/")
            client = OpenAI(
                base_url=f"https://{host_clean}/serving-endpoints",
                api_key=token,
            )
            return client, "databricks-meta-llama-3-3-70b-instruct"
    except Exception as e:
        st.sidebar.warning(f"Databricks LLM: {e}")

    # 2) Groq fallback
    groq_key = os.environ.get("GROQ_API_KEY", "")
    if groq_key:
        client = OpenAI(base_url="https://api.groq.com/openai/v1", api_key=groq_key)
        return client, "llama-3.3-70b-versatile"

    # 3) Gemini fallback
    gemini_key = os.environ.get("GEMINI_API_KEY", "")
    if gemini_key:
        client = OpenAI(
            base_url="https://generativelanguage.googleapis.com/v1beta/openai/",
            api_key=gemini_key,
        )
        return client, "gemini-2.0-flash"

    return None, None


# ═══════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════
import requests

def sarvam_audio_to_text(audio_bytes, lang="hi-IN"):
    sarvam_key = os.environ.get("SARVAM_API_KEY", "")
    if not sarvam_key:
        return ""
    try:
        url = "https://api.sarvam.ai/speech-to-text-translate"
        headers = {"api-subscription-key": sarvam_key}
        files = {"file": ("audio.wav", audio_bytes, "audio/wav")}
        response = requests.post(url, headers=headers, files=files, timeout=15)
        eng_text = ""
        if response.status_code == 200:
            eng_text = response.json().get("transcript", "")
            
        if not eng_text: return ""
        
        if lang == "hi-IN":
            return sarvam_translate(eng_text, source="en-IN", target="hi-IN")
        return eng_text
    except Exception as e:
        return ""

def sarvam_translate(text, source="hi-IN", target="en-IN"):
    sarvam_key = os.environ.get("SARVAM_API_KEY", "")
    if not sarvam_key:
        return text
    try:
        url = "https://api.sarvam.ai/translate"
        headers = {"api-subscription-key": sarvam_key, "Content-Type": "application/json"}
        payload = {"input": text, "source_language_code": source, "target_language_code": target, "speaker_gender": "Female", "mode": "formal", "model": "mayura:v1", "enable_preprocessing": True}
        response = requests.post(url, headers=headers, json=payload, timeout=15)
        if response.status_code == 200:
            return response.json().get("translated_text", text)
        return text
    except Exception:
        return text

def detect_live_outbreaks(df_triage, eps_km=10, min_samples=3):
    from sklearn.cluster import DBSCAN
    import math

    if df_triage.empty:
        return []
    
    df_recent = df_triage.copy()
    for c in ['latitude', 'longitude']:
        if c in df_recent.columns:
            df_recent[c] = pd.to_numeric(df_recent[c], errors='coerce')
    df_recent = df_recent.dropna(subset=['latitude', 'longitude'])
    if df_recent.empty: return []

    eps_degrees = eps_km / 111.0
    alerts = []
    
    for cluster_name in df_recent['symptom_cluster'].unique():
        df_cluster = df_recent[df_recent['symptom_cluster'] == cluster_name]
        if len(df_cluster) < min_samples: continue
        
        coords = df_cluster[['latitude', 'longitude']].values
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
                
                max_dist = 0
                for p in cluster_points:
                    d = math.sqrt((p[0] - centroid_lat)**2 + (p[1] - centroid_lng)**2) * 111
                    max_dist = max(max_dist, d)
                
                if case_count >= 10: severity = "RED"
                elif case_count >= 5: severity = "ORANGE"
                else: severity = "YELLOW"
                
                disease = {
                    "dengue_like": "Dengue Fever", "malaria_like": "Malaria",
                    "gastro_acute": "Acute Gastroenteritis / Cholera", "snakebite": "Snakebite Cluster",
                    "ari_pneumonia": "Respiratory Infection", "maternal_emergency": "Maternal Emergency"
                }.get(cluster_name, cluster_name)
                
                districts = ", ".join(cluster_cases['district'].unique()) if 'district' in cluster_cases.columns else "Unknown"
                
                alerts.append({
                    "alert_id": f"LIVE-ALERT-{cluster_name}-{cluster_id+1}",
                    "disease_suspected": disease, "symptom_cluster": cluster_name,
                    "centroid_lat": round(float(centroid_lat), 6), "centroid_lng": round(float(centroid_lng), 6),
                    "radius_km": round(float(max_dist), 2), "case_count": int(case_count),
                    "severity": severity, "affected_districts": districts, "status": "ACTIVE"
                })
    return alerts

def haversine_km(lat1, lng1, lat2, lng2):
    return math.sqrt(((float(lat1)-float(lat2))*111)**2 + ((float(lng1)-float(lng2))*102)**2)


def search_protocols(query, model, index, texts, diseases, sources, top_k=3):
    if model is None or index is None:
        return []
        
    if hasattr(model, 'encode'):
        query_vec = model.encode([query], normalize_embeddings=True).astype('float32')
    else:
        res = model.embeddings.create(model="databricks-bge-large-en", input=[query])
        query_vec = np.array([res.data[0].embedding]).astype('float32')
        
    scores, indices = index.search(query_vec, top_k)
    return [{"score": float(s), "disease": diseases[i], "source": sources[i], "text": texts[i]}
            for s, i in zip(scores[0], indices[0])]


def check_inventory(disease, patient_lat, patient_lng, facilities, inventory):
    disease_drug_map = {
        "Snakebite": ["Polyvalent Anti-Snake Venom (ASV)", "Normal Saline 0.9% (1L)", "Adrenaline (Epinephrine) 1mg/ml"],
        "Dengue Fever": ["Dengue NS1 Antigen Test Kit", "Normal Saline 0.9% (1L)", "Ringer Lactate (1L)", "Paracetamol 500mg"],
        "Malaria": ["Artesunate Injection 60mg", "Artemether-Lumefantrine Tablets"],
        "Acute Gastroenteritis": ["ORS Packets (WHO formula)", "Normal Saline 0.9% (1L)", "Ringer Lactate (1L)"],
        "Pneumonia": ["Amoxicillin 500mg", "Paracetamol 500mg"],
        "Obstetric Emergency": ["Oxytocin Injection 5IU", "Magnesium Sulphate Injection 50%"],
        "Rabies": ["Anti-Rabies Vaccine (ARV)", "Tetanus Toxoid Injection"],
    }
    required_drugs = disease_drug_map.get(disease, [])
    if not required_drugs or facilities is None or facilities.empty:
        return []

    fac = facilities.copy()
    fac['distance_km'] = fac.apply(lambda r: haversine_km(patient_lat, patient_lng, r['latitude'], r['longitude']), axis=1)
    fac = fac.sort_values('distance_km')

    results = []
    for _, f in fac.head(10).iterrows():
        f_inv = inventory[(inventory['facility_id'] == f['facility_id']) & (inventory['drug_name'].isin(required_drugs))]
        avail = f_inv[f_inv['stock_status'] == 'IN_STOCK']['drug_name'].tolist()
        miss = f_inv[f_inv['stock_status'] != 'IN_STOCK']['drug_name'].tolist()
        results.append({
            "name": f['facility_name'], "type": f['facility_type'], "district": f['district'],
            "distance_km": round(float(f['distance_km']),1),
            "lat": float(f['latitude']), "lng": float(f['longitude']),
            "beds": int(f['beds']) if pd.notna(f['beds']) else 0,
            "drugs_available": avail, "drugs_missing": miss, "ready": len(miss)==0,
        })
    results.sort(key=lambda x: (not x['ready'], x['distance_km']))
    return results


def generate_triage_response(symptoms, lat, lng, age, gender, protocols, facilities, outbreaks, client, model_name):
    proto_ctx = "\n".join([f"[{r['disease']}]: {r['text']}" for r in protocols[:3]])
    fac_ctx = ""
    for i, f in enumerate(facilities[:5]):
        fac_ctx += f"\n{i+1}. {f['name']} ({f['type']}) - {f['distance_km']}km - {'READY' if f['ready'] else 'MISSING: '+', '.join(f['drugs_missing'])}"

    outbreak_ctx = ""
    if outbreaks is not None and not outbreaks.empty:
        for _, a in outbreaks.iterrows():
            outbreak_ctx += f"\n- {a.get('severity','?')} ALERT: {a.get('disease_suspected','?')} ({a.get('case_count','?')} cases)"

    system = """You are Sanjeevani, an AI healthcare triage assistant for ASHA workers in rural Madhya Pradesh, India.
RULES:
- Start with SEVERITY LEVEL: MILD / MODERATE / SEVERE / CRITICAL
- Give suspected diagnosis
- List 3-5 actionable first-aid steps from MOHFW protocols with drug names and doses
- State which facility to go to, warn if OUT OF STOCK
- Flag any outbreak alerts
- Use very simple, easy-to-read "light" English language compatible with non-native speakers.
- End with referral criteria"""

    user = f"""SYMPTOMS: {symptoms}
PATIENT: Age {age or '?'}, Gender {gender or '?'}
LOCATION: ({lat}, {lng})

PROTOCOLS:
{proto_ctx}

NEARBY FACILITIES:
{fac_ctx}

OUTBREAK ALERTS: {outbreak_ctx or 'None'}

Provide triage assessment."""

    if client is None:
        d = protocols[0]['disease'] if protocols else "Unknown"
        f = facilities[0] if facilities else None
        return f"**SEVERITY: MODERATE** _(AI offline)_\n\n**Suspected:** {d}\n\n**Protocol:** {protocols[0]['text'][:300] if protocols else 'N/A'}...\n\n**Facility:** {f['name']+' ('+str(f['distance_km'])+' km)' if f else 'Nearest hospital'}"

    try:
        resp = client.chat.completions.create(
            model=model_name,
            messages=[{"role":"system","content":system},{"role":"user","content":user}],
            max_tokens=800, temperature=0.3,
        )
        return resp.choices[0].message.content
    except Exception as e:
        d = protocols[0]['disease'] if protocols else "Unknown"
        return f"**LLM Error:** {e}\n\n**Fallback - Suspected:** {d}"


# ═══════════════════════════════════════════════════════
# MAIN APP
# ═══════════════════════════════════════════════════════

def main():
    client, model_name = init_llm_client()
    embed_model, faiss_index, proto_texts, proto_diseases, proto_sources = init_faiss_index()

    facilities = load_facilities()
    inventory = load_inventory()
    triage_logs = load_triage_logs()
    outbreaks = load_outbreaks()

    # ─── Sidebar ───
    with st.sidebar:
        st.markdown("## 🏥 BHAASHA")
        st.markdown("*Population Health Routing Engine*")
        st.divider()
        page = st.radio("Navigate", [
            "🩺 Triage Console", "📊 Outbreak Dashboard",
            "💊 Inventory Status"
        ], label_visibility="collapsed")
        st.divider()
        st.markdown("### System Status")
        st.markdown(f"- LLM: {'🟢 '+str(model_name) if client else '🔴 Offline'}")
        st.markdown(f"- FAISS: {'🟢 '+str(len(proto_texts))+' chunks' if faiss_index else '🔴 Not loaded'}")
        st.markdown(f"- Facilities: {'🟢 '+str(len(facilities)) if not facilities.empty else '🔴 Empty'}")
        st.markdown(f"- Inventory: {'🟢 '+str(len(inventory)) if not inventory.empty else '🔴 Empty'}")
        if outbreaks is not None and not outbreaks.empty:
            st.markdown(f"- Outbreaks: 🔴 **{len(outbreaks)} ACTIVE**")

    # ═══ TRIAGE CONSOLE ═══
    if "Triage" in page:
        st.markdown("# 🩺 BHAASHA Triage Console")
        st.markdown("<p style='font-style: italic; color: #94a3b8; font-size: 1.1rem; margin-top: -15px;'>BHAASHA BHARAT KI AASHA</p>", unsafe_allow_html=True)
        st.markdown("*AI-powered health triage for ASHA workers - Indore Division, MP*")

        col1, col2 = st.columns([2, 1])
        with col1:
            st.markdown("### Report Symptoms")
            input_lang = st.radio("Patient Language:", ["Hindi (हिंदी)", "English"], horizontal=True)
            lang_code = "en-IN" if "English" in input_lang else "hi-IN"

            st.info("🎤 **Voice Input**: Use your browser's microphone or type below")
            
            audio_file = st.audio_input("Record Audio")
            
            example = st.selectbox("Quick examples:", [
                "(Type your own)",
                "teen din se tez bukhar, jodon me dard, shareer pe lal daane, ulti",
                "saanp ne kaata, soojan aa rahi hai, masoodhe se khoon",
                "bacche ko khansi 5 din se, saans tez, chhati dhans rahi hai, bukhar",
                "bahut zyada khoon beh raha hai delivery ke baad, BP gir raha hai",
                "dast aur ulti 2 din se, paani ki kami, pet me dard",
                "kutte ne kaata, khoon nikal raha hai, 2 ghante pehle",
            ])
            
            symptoms_text = ""
            if audio_file is not None:
                with st.spinner("Processing audio..."):
                    audio_bytes = audio_file.read()
                    symptoms_text = sarvam_audio_to_text(audio_bytes, lang=lang_code)
                    
            if not symptoms_text:
                symptoms_text = "" if example.startswith("(") else example
                
            symptoms_input = st.text_area(
                "Symptoms (Hindi or English):",
                value=symptoms_text,
                height=100, placeholder="Patient ko teen din se bukhar hai..."
            )
        with col2:
            st.markdown("### Patient Details")
            patient_age = st.number_input("Age", min_value=0, max_value=120, value=30)
            patient_gender = st.selectbox("Gender", ["M", "F", "Other"])
            st.markdown("### Location")
            try:
                from streamlit_geolocation import streamlit_geolocation
                loc = streamlit_geolocation()
                if loc and loc.get('latitude') is not None:
                    st.session_state.patient_lat = float(loc['latitude'])
                    st.session_state.patient_lng = float(loc['longitude'])
            except Exception:
                pass

            if "patient_lat" not in st.session_state:
                st.session_state.patient_lat = 22.72
                st.session_state.patient_lng = 75.86

            patient_lat = st.number_input("Latitude", value=st.session_state.patient_lat, format="%.4f")
            patient_lng = st.number_input("Longitude", value=st.session_state.patient_lng, format="%.4f")
            st.caption("Default: Indore city center")

        if st.button("🚨 **ANALYZE & TRIAGE**", type="primary", use_container_width=True):
            if not symptoms_input.strip():
                st.warning("Please enter symptoms first.")
            else:
                with st.spinner("Processing..."):
                    if lang_code == "hi-IN":
                        english_symptoms = sarvam_translate(symptoms_input, source="hi-IN", target="en-IN")
                        if not english_symptoms:
                            english_symptoms = symptoms_input
                    else:
                        english_symptoms = symptoms_input

                    proto_results = search_protocols(english_symptoms, embed_model, faiss_index, proto_texts, proto_diseases, proto_sources)
                    top_disease = proto_results[0]['disease'] if proto_results else "Unknown"
                    fac_results = check_inventory(top_disease, patient_lat, patient_lng, facilities, inventory)
                    ai_resp = generate_triage_response(
                        english_symptoms, patient_lat, patient_lng, patient_age, patient_gender,
                        proto_results, fac_results, outbreaks, client, model_name
                    )

                    import uuid as _uuid
                    log_id = str(_uuid.uuid4())
                    clean_symptoms = english_symptoms.replace("'", "''")
                    insert_query = f"INSERT INTO vc.sanjeevani.triage_logs (log_id, timestamp, asha_worker_id, patient_age, patient_gender, symptoms_raw, symptoms_extracted, symptom_cluster, latitude, longitude, district, severity_score) VALUES ('{log_id}', current_timestamp(), 'UI_SUBMIT', {patient_age}, '{patient_gender}', '{clean_symptoms}', '{clean_symptoms}', 'live-entry', {patient_lat}, {patient_lng}, 'Indore', 5)"
                    try:
                        run_sql(insert_query)
                        st.cache_data.clear()
                        st.success("✅ Case saved to Databricks Delta Lake.")
                    except Exception as e:
                        st.warning(f"Could not save to database: {e}")

                    if lang_code == "hi-IN":
                        translated_resp = sarvam_translate(ai_resp, source="en-IN", target="hi-IN")

                        st.divider()
                        st.markdown("### 🤖 AI Triage Assessment")
                        st.markdown(translated_resp)
                        with st.expander("Show Original English Medical Notes"):
                            st.markdown(ai_resp)
                    else:
                        st.divider()
                        st.markdown("### 🤖 AI Triage Assessment")
                        st.markdown(ai_resp)

                st.markdown("### 🏥 Facility Routing")
                for i, f in enumerate(fac_results[:5]):
                    if f['ready']:
                        st.success(f"✅ **{f['name']}** ({f['type']}) -- {f['distance_km']} km -- ALL DRUGS IN STOCK")
                    else:
                        st.error(f"⚠️ **{f['name']}** ({f['type']}) -- {f['distance_km']} km -- MISSING: {', '.join(f['drugs_missing'])}")
                    if i == 0 and not f['ready']:
                        st.warning("🚨 **NEAREST FACILITY LACKS REQUIRED DRUGS. Route to next!**")

                if fac_results:
                    st.markdown("### 🗺️ Map")
                    map_data = pd.DataFrame(
                        [{"lat": f['lat'], "lon": f['lng']} for f in fac_results[:5]]
                        + [{"lat": patient_lat, "lon": patient_lng}]
                    )
                    st.map(map_data)

                with st.expander("📋 Clinical Protocol References"):
                    for r in proto_results:
                        st.markdown(f"**{r['disease']}** (Score: {r['score']:.3f})")
                        st.markdown(f"> {r['text']}")
                        st.caption(f"Source: {r['source']}")
                        st.divider()

    # ═══ OUTBREAK DASHBOARD ═══
    elif "Outbreak" in page:
        st.markdown("# 📊 Outbreak Detection Dashboard")
        st.markdown("*Real-time DBSCAN geospatial clustering on the last 48-hours of live triage data.*")
        
        with st.spinner("Fetching latest triage data & computing clusters..."):
            fresh_triage = load_recent_triage_logs()
            live_outbreaks_list = detect_live_outbreaks(fresh_triage, eps_km=10, min_samples=3)
            outbreaks = pd.DataFrame(live_outbreaks_list) if live_outbreaks_list else pd.DataFrame()

        if not outbreaks.empty:
            cols = st.columns(4)
            cols[0].metric("Active Alerts", len(outbreaks))
            cols[1].metric("Total Cases", int(outbreaks['case_count'].sum()) if 'case_count' in outbreaks.columns else 0)
            red = len(outbreaks[outbreaks['severity']=='RED']) if 'severity' in outbreaks.columns else 0
            cols[2].metric("RED Alerts", red)
            cols[3].metric("Districts", outbreaks['affected_districts'].nunique() if 'affected_districts' in outbreaks.columns else 0)
            st.divider()

            for _, a in outbreaks.iterrows():
                sev = a.get('severity','?')
                css = {"RED":"alert-red","ORANGE":"alert-orange","YELLOW":"alert-yellow"}.get(sev,"")
                st.markdown(f"""<div class="{css}">
                    <h3>🚨 {a.get('disease_suspected','?')} -- {sev} ALERT</h3>
                    <p><b>Cases:</b> {a.get('case_count','?')} | <b>Radius:</b> {a.get('radius_km','?')} km | <b>Districts:</b> {a.get('affected_districts','?')}</p>
                </div>""", unsafe_allow_html=True)

            if 'centroid_lat' in outbreaks.columns:
                st.markdown("### Cluster Map")
                st.map(outbreaks.rename(columns={"centroid_lat":"lat","centroid_lng":"lon"}))
        else:
            st.info("No outbreak alerts. Run notebook 02 to analyze triage logs.")

        if triage_logs is not None and not triage_logs.empty and 'symptom_cluster' in triage_logs.columns:
            st.divider()
            st.markdown("### Triage Distribution (7 days)")
            st.bar_chart(triage_logs['symptom_cluster'].value_counts())

    # ═══ INVENTORY STATUS ═══
    elif "Inventory" in page:
        st.markdown("# 💊 Drug Inventory Status")
        if inventory is not None and not inventory.empty:
            cols = st.columns(4)
            oos = len(inventory[inventory['stock_status']=='OUT_OF_STOCK'])
            low = len(inventory[inventory['stock_status']=='LOW'])
            crit = len(inventory[(inventory['stock_status']=='OUT_OF_STOCK') & (inventory['critical_flag'].astype(str).str.lower()=='true')])
            cols[0].metric("Total Items", f"{len(inventory):,}")
            cols[1].metric("Out of Stock", oos)
            cols[2].metric("Low Stock", low)
            cols[3].metric("Critical Shortages", crit)
            st.divider()

            c1, c2, c3 = st.columns(3)
            ftype = c1.multiselect("Facility Type", ["DH","CHC","PHC","SC"], default=["DH","CHC","PHC"])
            stock = c2.multiselect("Stock Status", ["OUT_OF_STOCK","LOW","IN_STOCK"], default=["OUT_OF_STOCK","LOW"])
            crit_only = c3.checkbox("Critical Only", value=True)

            merged = inventory.merge(facilities[['facility_id','facility_name','facility_type','district']], on='facility_id', how='left') if not facilities.empty else inventory
            mask = pd.Series([True]*len(merged))
            if 'facility_type' in merged.columns:
                mask = mask & merged['facility_type'].isin(ftype)
            mask = mask & merged['stock_status'].isin(stock)
            if crit_only and 'critical_flag' in merged.columns:
                mask = mask & (merged['critical_flag'].astype(str).str.lower()=='true')
            st.dataframe(merged[mask].sort_values(['stock_status','drug_name']), use_container_width=True, height=500)

            if 'district' in merged.columns:
                st.markdown("### Shortages by District")
                d_oos = merged[merged['stock_status']=='OUT_OF_STOCK'].groupby('district').size()
                if len(d_oos) > 0:
                    st.bar_chart(d_oos)


if __name__ == "__main__":
    main()
