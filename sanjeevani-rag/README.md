# BHAASHA: Population Health Routing Engine

### What it does
BHAASHA is a voice-enabled, multi-lingual triage console that empowers ASHA workers to speak patient symptoms in native Indian languages and instantly receive localized medical guidance, matching them to properly stocked nearby facilities. Under the hood, every submission streams into a Databricks Lakehouse which continuously runs real-time DBSCAN machine learning to map emerging disease outbreaks across districts.

### Architecture
```mermaid
graph TD
    A[ASHA Worker] -->|Voice Input (Hindi/Local)| B(Sarvam AI Speech-to-Text)
    B -->|Translated English| C[BHAASHA Streamlit App]
    C -->|Vector Search| D[(Databricks FAISS RAG)]
    D -->|Clinical Protocols| E[Databricks Foundation Model: Llama-3 70B]
    C -.->|SQL Check| F[(Databricks Unity Catalog)]
    F --> G[Inventory & Facility Tables]
    E -->|Triage Output| C
    C -->|Real-time Insert| H[(Delta Lake: triage_logs)]
    H -->|DBSCAN Clustering| I[Live Outbreak Dashboard]
```

### Databricks Technologies & Models Used
* **Databricks Serverless Apps**: Hosts the interactive `app.py` Streamlit frontend.
* **Databricks Unity Catalog & Delta Lake**: Stores the `health_facilities`, `drug_inventory`, and `triage_logs` as ACID-compliant tables.
* **Databricks Foundation Model APIs**: Utilizes `databricks-meta-llama-3-3-70b-instruct` for intelligent medical protocol summarization.
* **Databricks SQL Statement Execution API**: Powers the real-time Streamlit <-> Delta Lake connection.
* **Sarvam AI**: `speech-to-text-translate` endpoint for handling rural localized audio inputs.
* **Scikit-Learn (DBSCAN)**: For dynamic geoclustering of disease outbreaks.

### How to Run (Exact Commands)
**1. Environment Setup**
Ensure you have Databricks CLI installed and configured.
```bash
pip install -r app/requirements.txt
```

**2. Databricks Table Ingestion (One Time)**
Import the notebooks to your Databricks Workspace and click **"Run All"**:
* `notebooks/01_setup_and_ingest_workspace.py` 

**3. Run the Databricks App Locally**
To test the web interface locally before pushing to serverless:
```bash
export DATABRICKS_HOST="YOUR_URL"
export DATABRICKS_TOKEN="YOUR_PAT"
export SARVAM_API_KEY="YOUR_KEY"
cd app
streamlit run app.py
```

### Demo Steps
1. Navigate to the **Triage Console** tab.
2. Select **"Hindi"** from the Patient Language toggle.
3. Click the **Microphone** and say *"Patient ko bahut tez bukhar hai aur jodon me dard ho raha hai"* (Or type it into the symptom box) and hit **ANALYZE & TRIAGE**.
4. Observe the App extract English symptoms, query Llama-3, check Databricks inventory, and translate the finalized medical assessment back into Hindi.
5. Click over to the **Outbreak Dashboard** tab.
6. Observe that the case was successfully pushed to Databricks Delta Lake and immediately analyzed via DBSCAN, dynamically adjusting the map's RED/YELLOW health alert radius to reflect the new patient data.
