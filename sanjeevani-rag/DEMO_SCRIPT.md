# BHAASHA: 2-Minute Demo Video Script

**Timing:** ~120 seconds  
**Tone:** Professional, urgent, and focused on real-world impact and Databricks architecture.

---

### Segment 1: The Problem (0:00 - 0:20)
**[Visual: Title Slide with BHAASHA Logo and the Indian rural healthcare context]**
**Speaker:** "In rural India, frontline healthcare workers—called ASHAs—face two massive bottlenecks: a language barrier when accessing complex medical protocols, and delays in routing critical patients to correctly stocked hospitals. By the time regional health officers notice a cluster of symptoms, an epidemic like Dengue has already broken out."

### Segment 2: The Solution Architecture (0:20 - 0:40)
**[Visual: Screen recording showing the Architecture Diagram from the README]**
**Speaker:** "Enter BHAASHA. We built a voice-enabled, multi-lingual triage engine powered completely by the Databricks Lakehouse architecture. We use Sarvam AI for native Indian language transcription, Databricks Delta Lake and Unity Catalog for storing patient and inventory data natively, and the Databricks Foundation Llama-3 API for drawing real-time medical assessments."

### Segment 3: Live Demo - The Triage Console (0:40 - 1:20)
**[Visual: Screen recording of the BHAASHA Streamlit App, starting on the Triage Console tab]**
**Speaker:** "Let’s look at a live demo. An ASHA worker in Madhya Pradesh discovers a patient with high fever and joint pain. Selecting 'Hindi' as the output language, the worker simply presses the microphone and speaks natively."
**[Action: Click the microphone icon, say "Patient ko bahut tez bukhar hai aur jodon me dard ho raha hai", and hit ANALYZE]**
**Speaker:** "Behind the scenes, we extract the symptoms to medical English, search our clinical protocol FAISS index, and prompt Databricks Llama-3. The AI instantly returns a triage assessment and intelligently routes the patient to the nearest Primary Health Center that currently has the required drugs in stock, checking live against Unity Catalog tables. Finally, it translates the entire medical readout back into Hindi for the rural worker."

### Segment 4: Live Demo - Outbreak Analytics (1:20 - 1:45)
**[Visual: Switch to the 'Outbreak Dashboard' tab. Emphasize the live mapping and metrics]**
**Speaker:** "But we don't just route the patient. The moment the Triage button was clicked, that data was written in real-time to our Delta Lake via the Databricks SQL API."
**[Action: Highlight the map and the 'Red Alerts' metrics]**
**Speaker:** "Regional officers use this Outbreak Dashboard. Because our Delta Lake is always current, the app continuously runs a Scikit-Learn DBSCAN clustering algorithm over the last 48 hours of data. The exact Dengue symptom cluster from our patient just pushed this district into a RED alert, displaying the live radius dynamically."

### Segment 5: The Impact (1:45 - 2:00)
**[Visual: Final slide summarizing technologies: Databricks Serverless, Delta Lake, Sarvam AI]**
**Speaker:** "By combining any-language voice capabilities with the ACID-compliance and AI power of Databricks Serverless Apps, BHAASHA transforms rural data entry into real-time, life-saving epidemiological action. Thank you."
