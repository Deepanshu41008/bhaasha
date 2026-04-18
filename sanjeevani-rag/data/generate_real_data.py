"""
Generate realistic healthcare data for Sanjeevani-RAG
Focused on Indore Division, Madhya Pradesh
Uses real facility names, real drug lists, real disease patterns
"""
import csv
import random
import uuid
from datetime import datetime, timedelta

random.seed(42)

# ============================================================
# REAL Health Facilities - Indore Division, MP
# Based on actual PHC/CHC/DH locations from NHM MP data
# ============================================================

FACILITIES = [
    # District Hospitals
    {"name": "MY Hospital Indore", "type": "DH", "district": "Indore", "block": "Indore Urban", "lat": 22.7196, "lng": 75.8577, "beds": 500},
    {"name": "CHL Hospital Indore", "type": "DH", "district": "Indore", "block": "Indore Urban", "lat": 22.7241, "lng": 75.8839, "beds": 200},
    {"name": "District Hospital Dewas", "type": "DH", "district": "Dewas", "block": "Dewas", "lat": 22.9623, "lng": 76.0508, "beds": 150},
    {"name": "District Hospital Ujjain", "type": "DH", "district": "Ujjain", "block": "Ujjain", "lat": 23.1765, "lng": 75.7885, "beds": 200},
    {"name": "District Hospital Dhar", "type": "DH", "district": "Dhar", "block": "Dhar", "lat": 22.5972, "lng": 75.2997, "beds": 120},
    {"name": "District Hospital Khandwa", "type": "DH", "district": "Khandwa", "block": "Khandwa", "lat": 21.8243, "lng": 76.3483, "beds": 130},
    {"name": "District Hospital Khargone", "type": "DH", "district": "Khargone", "block": "Khargone", "lat": 21.8242, "lng": 75.6145, "beds": 100},
    {"name": "District Hospital Burhanpur", "type": "DH", "district": "Burhanpur", "block": "Burhanpur", "lat": 21.3092, "lng": 76.2256, "beds": 80},
    # Community Health Centres
    {"name": "CHC Mhow", "type": "CHC", "district": "Indore", "block": "Mhow", "lat": 22.5548, "lng": 75.7633, "beds": 30},
    {"name": "CHC Sanwer", "type": "CHC", "district": "Indore", "block": "Sanwer", "lat": 22.9714, "lng": 75.8266, "beds": 30},
    {"name": "CHC Depalpur", "type": "CHC", "district": "Indore", "block": "Depalpur", "lat": 22.8547, "lng": 75.5366, "beds": 30},
    {"name": "CHC Hatod", "type": "CHC", "district": "Indore", "block": "Hatod", "lat": 22.7733, "lng": 75.7350, "beds": 20},
    {"name": "CHC Manpur", "type": "CHC", "district": "Indore", "block": "Manpur", "lat": 22.4343, "lng": 75.6200, "beds": 20},
    {"name": "CHC Sonkatch", "type": "CHC", "district": "Dewas", "block": "Sonkatch", "lat": 22.9761, "lng": 76.3608, "beds": 30},
    {"name": "CHC Khategaon", "type": "CHC", "district": "Dewas", "block": "Khategaon", "lat": 22.5966, "lng": 76.9107, "beds": 25},
    {"name": "CHC Bagli", "type": "CHC", "district": "Dewas", "block": "Bagli", "lat": 22.6358, "lng": 76.3454, "beds": 20},
    {"name": "CHC Mahidpur", "type": "CHC", "district": "Ujjain", "block": "Mahidpur", "lat": 23.4785, "lng": 75.6517, "beds": 30},
    {"name": "CHC Tarana", "type": "CHC", "district": "Ujjain", "block": "Tarana", "lat": 23.3281, "lng": 76.0317, "beds": 25},
    {"name": "CHC Badnawar", "type": "CHC", "district": "Dhar", "block": "Badnawar", "lat": 23.0210, "lng": 75.6261, "beds": 25},
    {"name": "CHC Dharampuri", "type": "CHC", "district": "Dhar", "block": "Dharampuri", "lat": 22.1491, "lng": 75.3440, "beds": 20},
    {"name": "CHC Nepanagar", "type": "CHC", "district": "Burhanpur", "block": "Nepanagar", "lat": 21.4571, "lng": 76.3919, "beds": 20},
    # Primary Health Centres
    {"name": "PHC Rau", "type": "PHC", "district": "Indore", "block": "Indore Urban", "lat": 22.6770, "lng": 75.8103, "beds": 6},
    {"name": "PHC Betma", "type": "PHC", "district": "Indore", "block": "Mhow", "lat": 22.6839, "lng": 75.6140, "beds": 6},
    {"name": "PHC Simrol", "type": "PHC", "district": "Indore", "block": "Mhow", "lat": 22.5391, "lng": 75.8947, "beds": 6},
    {"name": "PHC Gautampura", "type": "PHC", "district": "Indore", "block": "Depalpur", "lat": 22.8643, "lng": 75.4955, "beds": 6},
    {"name": "PHC Kampel", "type": "PHC", "district": "Indore", "block": "Sanwer", "lat": 22.8990, "lng": 75.8985, "beds": 6},
    {"name": "PHC Manasa", "type": "PHC", "district": "Indore", "block": "Manpur", "lat": 22.4150, "lng": 75.5800, "beds": 4},
    {"name": "PHC Tonk Khurd", "type": "PHC", "district": "Dewas", "block": "Dewas", "lat": 22.8850, "lng": 76.1200, "beds": 6},
    {"name": "PHC Kannod", "type": "PHC", "district": "Dewas", "block": "Kannod", "lat": 22.6700, "lng": 76.7433, "beds": 6},
    {"name": "PHC Nagda", "type": "PHC", "district": "Ujjain", "block": "Nagda", "lat": 23.4572, "lng": 75.4155, "beds": 6},
    {"name": "PHC Barnagar", "type": "PHC", "district": "Ujjain", "block": "Barnagar", "lat": 23.0490, "lng": 75.5683, "beds": 6},
    {"name": "PHC Sardarpur", "type": "PHC", "district": "Dhar", "block": "Sardarpur", "lat": 22.6900, "lng": 74.8900, "beds": 4},
    {"name": "PHC Manawar", "type": "PHC", "district": "Dhar", "block": "Manawar", "lat": 22.2350, "lng": 75.0885, "beds": 4},
    {"name": "PHC Pandhana", "type": "PHC", "district": "Khandwa", "block": "Pandhana", "lat": 21.6853, "lng": 76.2072, "beds": 6},
    {"name": "PHC Punasa", "type": "PHC", "district": "Khandwa", "block": "Punasa", "lat": 22.2369, "lng": 76.3760, "beds": 4},
    {"name": "PHC Kasrawad", "type": "PHC", "district": "Khargone", "block": "Kasrawad", "lat": 21.9200, "lng": 75.5947, "beds": 4},
    {"name": "PHC Bhikangaon", "type": "PHC", "district": "Khargone", "block": "Bhikangaon", "lat": 21.5555, "lng": 75.9505, "beds": 4},
    # Sub-Centres (some key ones)
    {"name": "SC Limbodi", "type": "SC", "district": "Indore", "block": "Sanwer", "lat": 22.9350, "lng": 75.7950, "beds": 0},
    {"name": "SC Palda", "type": "SC", "district": "Indore", "block": "Indore Urban", "lat": 22.7520, "lng": 75.8230, "beds": 0},
    {"name": "SC Pipliyahana", "type": "SC", "district": "Indore", "block": "Indore Urban", "lat": 22.7398, "lng": 75.8655, "beds": 0},
    {"name": "SC Khajrana", "type": "SC", "district": "Indore", "block": "Indore Urban", "lat": 22.7150, "lng": 75.9020, "beds": 0},
    {"name": "SC Rangwasa", "type": "SC", "district": "Indore", "block": "Mhow", "lat": 22.5800, "lng": 75.7100, "beds": 0},
    {"name": "SC Tejpur", "type": "SC", "district": "Dewas", "block": "Dewas", "lat": 22.9300, "lng": 76.0800, "beds": 0},
    {"name": "SC Unhel", "type": "SC", "district": "Ujjain", "block": "Ujjain", "lat": 23.2100, "lng": 75.8200, "beds": 0},
]

# ============================================================
# REAL Essential Drug List - Based on NLEM 2022 India (PHC level)
# ============================================================

DRUGS = [
    # Anti-Snake Venom - CRITICAL
    {"name": "Polyvalent Anti-Snake Venom (ASV)", "category": "Antivenom", "unit": "vials", "nlem": "P", "critical": True},
    # Antimalarials
    {"name": "Artesunate Injection 60mg", "category": "Antimalarial", "unit": "vials", "nlem": "P", "critical": True},
    {"name": "Artemether-Lumefantrine Tablets", "category": "Antimalarial", "unit": "tablets", "nlem": "P", "critical": True},
    {"name": "Chloroquine Phosphate 250mg", "category": "Antimalarial", "unit": "tablets", "nlem": "P", "critical": False},
    {"name": "Primaquine 7.5mg", "category": "Antimalarial", "unit": "tablets", "nlem": "P", "critical": False},
    # Dengue Management
    {"name": "Dengue NS1 Antigen Test Kit", "category": "Diagnostic", "unit": "kits", "nlem": "P", "critical": True},
    {"name": "Platelet Rich Plasma", "category": "Blood Product", "unit": "units", "nlem": "S", "critical": True},
    {"name": "Normal Saline 0.9% (1L)", "category": "IV Fluid", "unit": "bottles", "nlem": "P", "critical": True},
    {"name": "Ringer Lactate (1L)", "category": "IV Fluid", "unit": "bottles", "nlem": "P", "critical": True},
    # ORS / Diarrhoea
    {"name": "ORS Packets (WHO formula)", "category": "ORS", "unit": "packets", "nlem": "P", "critical": True},
    {"name": "Zinc Sulphate Dispersible 20mg", "category": "Micronutrient", "unit": "tablets", "nlem": "P", "critical": False},
    {"name": "Metronidazole 400mg", "category": "Antiprotozoal", "unit": "tablets", "nlem": "P", "critical": False},
    # Antibiotics
    {"name": "Amoxicillin 500mg", "category": "Antibiotic", "unit": "capsules", "nlem": "P", "critical": False},
    {"name": "Azithromycin 500mg", "category": "Antibiotic", "unit": "tablets", "nlem": "P", "critical": False},
    {"name": "Ciprofloxacin 500mg", "category": "Antibiotic", "unit": "tablets", "nlem": "P", "critical": False},
    {"name": "Cotrimoxazole Paediatric", "category": "Antibiotic", "unit": "tablets", "nlem": "P", "critical": False},
    {"name": "Gentamicin Injection 80mg", "category": "Antibiotic", "unit": "ampoules", "nlem": "P", "critical": True},
    {"name": "Ceftriaxone Injection 1g", "category": "Antibiotic", "unit": "vials", "nlem": "S", "critical": True},
    # Maternal / Emergency
    {"name": "Oxytocin Injection 5IU", "category": "Obstetric", "unit": "ampoules", "nlem": "P", "critical": True},
    {"name": "Misoprostol 200mcg", "category": "Obstetric", "unit": "tablets", "nlem": "P", "critical": True},
    {"name": "Magnesium Sulphate Injection 50%", "category": "Anticonvulsant", "unit": "ampoules", "nlem": "P", "critical": True},
    {"name": "Adrenaline (Epinephrine) 1mg/ml", "category": "Emergency", "unit": "ampoules", "nlem": "P", "critical": True},
    {"name": "Hydrocortisone Injection 100mg", "category": "Corticosteroid", "unit": "vials", "nlem": "P", "critical": True},
    {"name": "Atropine Sulphate 0.6mg/ml", "category": "Anticholinergic", "unit": "ampoules", "nlem": "P", "critical": True},
    # Pain / Fever
    {"name": "Paracetamol 500mg", "category": "Analgesic", "unit": "tablets", "nlem": "P", "critical": False},
    {"name": "Ibuprofen 400mg", "category": "NSAID", "unit": "tablets", "nlem": "P", "critical": False},
    {"name": "Diclofenac Sodium 50mg", "category": "NSAID", "unit": "tablets", "nlem": "P", "critical": False},
    # Respiratory
    {"name": "Salbutamol Inhaler 100mcg", "category": "Bronchodilator", "unit": "inhalers", "nlem": "P", "critical": False},
    {"name": "Dexamethasone Injection 4mg", "category": "Corticosteroid", "unit": "ampoules", "nlem": "P", "critical": True},
    {"name": "Prednisolone 5mg", "category": "Corticosteroid", "unit": "tablets", "nlem": "P", "critical": False},
    # Diabetes / Hypertension (common in rural)
    {"name": "Metformin 500mg", "category": "Antidiabetic", "unit": "tablets", "nlem": "P", "critical": False},
    {"name": "Glimepiride 1mg", "category": "Antidiabetic", "unit": "tablets", "nlem": "P", "critical": False},
    {"name": "Amlodipine 5mg", "category": "Antihypertensive", "unit": "tablets", "nlem": "P", "critical": False},
    {"name": "Enalapril 5mg", "category": "Antihypertensive", "unit": "tablets", "nlem": "P", "critical": False},
    # Tetanus
    {"name": "Tetanus Toxoid Injection", "category": "Vaccine", "unit": "doses", "nlem": "P", "critical": True},
    {"name": "Anti-Tetanus Serum (ATS)", "category": "Antiserum", "unit": "vials", "nlem": "P", "critical": True},
    # Rabies
    {"name": "Anti-Rabies Vaccine (ARV)", "category": "Vaccine", "unit": "doses", "nlem": "P", "critical": True},
    {"name": "Rabies Immunoglobulin (RIG)", "category": "Immunoglobulin", "unit": "vials", "nlem": "S", "critical": True},
    # Poisoning
    {"name": "Activated Charcoal Powder", "category": "Antidote", "unit": "sachets", "nlem": "P", "critical": True},
    {"name": "Pralidoxime (PAM) Injection", "category": "Antidote", "unit": "vials", "nlem": "S", "critical": True},
]

# ============================================================
# Symptom clusters for outbreak detection
# ============================================================

SYMPTOM_CLUSTERS = {
    "dengue_like": {
        "symptoms": ["high fever", "severe headache", "joint pain", "muscle pain", "rash", "retro-orbital pain", "nausea", "vomiting"],
        "hindi": ["tez bukhar", "sir me dard", "jodon me dard", "shareer me dard", "lal daane", "ulti", "ji machlana"],
        "disease": "Dengue Fever",
        "weight": 0.25  # monsoon weighted
    },
    "malaria_like": {
        "symptoms": ["intermittent fever", "chills", "rigors", "sweating", "headache", "body ache", "nausea"],
        "hindi": ["thandi lagna", "kaanpna", "pasina", "bukhar aata jaata", "badan dard"],
        "disease": "Malaria (P. falciparum/vivax)",
        "weight": 0.20
    },
    "gastro_acute": {
        "symptoms": ["watery diarrhea", "vomiting", "abdominal cramps", "dehydration", "fever"],
        "hindi": ["dast", "ulti", "pet me dard", "paani ki kami", "bukhar"],
        "disease": "Acute Gastroenteritis / Cholera suspect",
        "weight": 0.20
    },
    "snakebite": {
        "symptoms": ["bite mark", "swelling at bite site", "pain", "bleeding from gums", "blurred vision", "difficulty breathing", "ptosis"],
        "hindi": ["saanp ne kaata", "soojan", "dard", "masoodhe se khoon", "dhundla dikhna", "saans me taklif"],
        "disease": "Snakebite (Viper/Krait suspected)",
        "weight": 0.10
    },
    "ari_pneumonia": {
        "symptoms": ["cough", "rapid breathing", "chest indrawing", "fever", "difficulty breathing", "wheezing"],
        "hindi": ["khansi", "tez saans", "chhati dhansna", "bukhar", "saans me taklif"],
        "disease": "Acute Respiratory Infection / Pneumonia",
        "weight": 0.15
    },
    "maternal_emergency": {
        "symptoms": ["heavy bleeding", "severe abdominal pain", "convulsions", "high BP", "headache", "swelling face hands"],
        "hindi": ["bahut zyada khoon", "pet me bahut dard", "daurey", "BP badha hua", "sir dard", "chehra haath sooja hua"],
        "disease": "Obstetric Emergency (PPH/Eclampsia)",
        "weight": 0.10
    },
}

# ============================================================
# GENERATE: health_facilities.csv
# ============================================================

print("Generating health_facilities.csv...")
with open("c:/Users/gupta/antigravity/new/sanjeevani-rag/data/health_facilities.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["facility_id", "facility_name", "facility_type", "state", "district", "block", "latitude", "longitude", "beds", "ownership", "phone", "operating_hours"])
    writer.writeheader()
    for i, fac in enumerate(FACILITIES):
        writer.writerow({
            "facility_id": f"MP-{fac['district'][:3].upper()}-{fac['type']}-{i+1:03d}",
            "facility_name": fac["name"],
            "facility_type": fac["type"],
            "state": "Madhya Pradesh",
            "district": fac["district"],
            "block": fac["block"],
            "latitude": fac["lat"],
            "longitude": fac["lng"],
            "beds": fac["beds"],
            "ownership": "Government",
            "phone": f"0731-{random.randint(2000000, 2999999)}",
            "operating_hours": "24x7" if fac["type"] in ("DH", "CHC") else "8AM-5PM"
        })
print(f"  -> {len(FACILITIES)} facilities written")

# ============================================================
# GENERATE: drug_inventory.csv
# ============================================================

print("Generating drug_inventory.csv...")
inventory_rows = []
for i, fac in enumerate(FACILITIES):
    fac_id = f"MP-{fac['district'][:3].upper()}-{fac['type']}-{i+1:03d}"
    for drug in DRUGS:
        # Sub-centres get very limited drugs
        if fac["type"] == "SC" and drug["nlem"] != "P":
            continue
        if fac["type"] == "SC" and drug["category"] in ("Blood Product", "Immunoglobulin", "Antidote"):
            continue
        
        # PHCs don't get secondary-level drugs
        if fac["type"] == "PHC" and drug["nlem"] == "S":
            continue
        
        # Determine stock based on facility type
        if fac["type"] == "DH":
            max_stock = 200 if not drug["critical"] else 100
        elif fac["type"] == "CHC":
            max_stock = 80 if not drug["critical"] else 40
        elif fac["type"] == "PHC":
            max_stock = 30 if not drug["critical"] else 15
        else:  # SC
            max_stock = 10
        
        # Create realistic stock issues
        # ~15% of critical drugs at PHC/SC are out of stock
        # ~5% at CHC, ~2% at DH
        out_of_stock_chance = {
            "SC": 0.25, "PHC": 0.15, "CHC": 0.05, "DH": 0.02
        }[fac["type"]]
        
        if drug["critical"] and random.random() < out_of_stock_chance:
            qty = 0
            status = "OUT_OF_STOCK"
        elif random.random() < 0.10:
            qty = random.randint(1, max(1, max_stock // 10))
            status = "LOW"
        else:
            qty = random.randint(max_stock // 3, max_stock)
            status = "IN_STOCK"
        
        restock_days_ago = random.randint(1, 60)
        
        inventory_rows.append({
            "facility_id": fac_id,
            "drug_name": drug["name"],
            "drug_category": drug["category"],
            "nlem_level": drug["nlem"],
            "quantity_available": qty,
            "unit": drug["unit"],
            "last_restocked": (datetime.now() - timedelta(days=restock_days_ago)).strftime("%Y-%m-%d"),
            "stock_status": status,
            "critical_flag": drug["critical"],
        })

with open("c:/Users/gupta/antigravity/new/sanjeevani-rag/data/drug_inventory.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=list(inventory_rows[0].keys()))
    writer.writeheader()
    writer.writerows(inventory_rows)
print(f"  -> {len(inventory_rows)} inventory records written")

# ============================================================
# GENERATE: triage_logs.csv (simulated last 7 days)
# ============================================================

print("Generating triage_logs.csv...")
ASHA_WORKERS = [f"ASHA-{d[:3].upper()}-{j:03d}" for d in ["Indore", "Dewas", "Ujjain", "Dhar", "Khandwa", "Khargone", "Burhanpur"] for j in range(1, 16)]

triage_rows = []
now = datetime.now()

# Create a DENGUE outbreak cluster near Mhow (to be detected by DBSCAN)
outbreak_center_lat, outbreak_center_lng = 22.555, 75.763
for k in range(75):
    ts = now - timedelta(hours=random.randint(2, 44))
    cluster_info = SYMPTOM_CLUSTERS["dengue_like"]
    n_symptoms = random.randint(3, 5)
    selected_symptoms = random.sample(cluster_info["symptoms"], n_symptoms)
    selected_hindi = random.sample(cluster_info["hindi"], min(n_symptoms, len(cluster_info["hindi"])))
    
    lat_noise = random.gauss(0, 0.03)  # ~3km spread
    lng_noise = random.gauss(0, 0.03)
    
    triage_rows.append({
        "log_id": str(uuid.uuid4()),
        "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
        "asha_worker_id": random.choice([a for a in ASHA_WORKERS if "IND" in a]),
        "patient_age": random.randint(5, 65),
        "patient_gender": random.choice(["M", "F"]),
        "symptoms_raw": ", ".join(selected_hindi),
        "symptoms_extracted": ", ".join(selected_symptoms),
        "symptom_cluster": "dengue_like",
        "latitude": round(outbreak_center_lat + lat_noise, 6),
        "longitude": round(outbreak_center_lng + lng_noise, 6),
        "district": "Indore",
        "severity_score": random.randint(3, 5),
    })

# Create a GASTRO cluster near Dewas
outbreak2_lat, outbreak2_lng = 22.96, 76.05
for k in range(50):
    ts = now - timedelta(hours=random.randint(4, 40))
    cluster_info = SYMPTOM_CLUSTERS["gastro_acute"]
    n_symptoms = random.randint(3, 4)
    selected_symptoms = random.sample(cluster_info["symptoms"], n_symptoms)
    selected_hindi = random.sample(cluster_info["hindi"], min(n_symptoms, len(cluster_info["hindi"])))
    
    triage_rows.append({
        "log_id": str(uuid.uuid4()),
        "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
        "asha_worker_id": random.choice([a for a in ASHA_WORKERS if "DEW" in a]),
        "patient_age": random.randint(2, 50),
        "patient_gender": random.choice(["M", "F"]),
        "symptoms_raw": ", ".join(selected_hindi),
        "symptoms_extracted": ", ".join(selected_symptoms),
        "symptom_cluster": "gastro_acute",
        "latitude": round(outbreak2_lat + random.gauss(0, 0.02), 6),
        "longitude": round(outbreak2_lng + random.gauss(0, 0.02), 6),
        "district": "Dewas",
        "severity_score": random.randint(2, 4),
    })

# Scattered cases (normal background noise)
for k in range(500):
    cluster_name = random.choices(
        list(SYMPTOM_CLUSTERS.keys()),
        weights=[c["weight"] for c in SYMPTOM_CLUSTERS.values()]
    )[0]
    cluster_info = SYMPTOM_CLUSTERS[cluster_name]
    
    ts = now - timedelta(hours=random.randint(1, 168))  # last 7 days
    fac = random.choice(FACILITIES)
    n_symptoms = random.randint(2, 4)
    selected_symptoms = random.sample(cluster_info["symptoms"], min(n_symptoms, len(cluster_info["symptoms"])))
    selected_hindi = random.sample(cluster_info["hindi"], min(n_symptoms, len(cluster_info["hindi"])))
    
    triage_rows.append({
        "log_id": str(uuid.uuid4()),
        "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
        "asha_worker_id": random.choice(ASHA_WORKERS),
        "patient_age": random.randint(1, 80),
        "patient_gender": random.choice(["M", "F"]),
        "symptoms_raw": ", ".join(selected_hindi),
        "symptoms_extracted": ", ".join(selected_symptoms),
        "symptom_cluster": cluster_name,
        "latitude": round(fac["lat"] + random.gauss(0, 0.05), 6),
        "longitude": round(fac["lng"] + random.gauss(0, 0.05), 6),
        "district": fac["district"],
        "severity_score": random.randint(1, 5),
    })

with open("c:/Users/gupta/antigravity/new/sanjeevani-rag/data/triage_logs.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=list(triage_rows[0].keys()))
    writer.writeheader()
    writer.writerows(triage_rows)
print(f"  -> {len(triage_rows)} triage logs written")

# ============================================================
# GENERATE: clinical_protocols.csv (chunked text from real guidelines)
# ============================================================

print("Generating clinical_protocols.csv...")
PROTOCOL_CHUNKS = [
    # SNAKEBITE PROTOCOL - Based on WHO/MOHFW Guidelines
    {"source": "MOHFW_Snakebite_Guidelines_2024", "disease": "Snakebite", "chunk": "SNAKEBITE FIRST AID: 1. Reassure and immobilize the patient. Keep the bitten limb below heart level. 2. Do NOT apply tourniquet, do NOT cut the wound, do NOT suck the venom. 3. Remove rings, watches, tight clothing from bitten limb. 4. Transport patient to nearest health facility with Anti-Snake Venom (ASV) immediately. 5. Note the time of bite and description of snake if possible."},
    {"source": "MOHFW_Snakebite_Guidelines_2024", "disease": "Snakebite", "chunk": "ASV ADMINISTRATION: Polyvalent Anti-Snake Venom is effective against Big Four: Cobra, Krait, Russell's Viper, Saw-scaled Viper. Dose: Start with 10 vials (100ml) ASV diluted in Normal Saline, IV infusion over 1 hour. Premedicate with Adrenaline 0.25mg SC, Hydrocortisone 200mg IV, and Chlorpheniramine 10mg IV. Watch for anaphylaxis. If signs of envenomation persist after 1-2 hours, repeat 10 vials."},
    {"source": "MOHFW_Snakebite_Guidelines_2024", "disease": "Snakebite", "chunk": "SIGNS OF ENVENOMATION - HEMOTOXIC (Viper): Local swelling progressing beyond bite site, bleeding from gums/injection sites, blood in urine, blood not clotting (20 Minute Whole Blood Clotting Test - 20WBCT). NEUROTOXIC (Krait/Cobra): Ptosis (drooping eyelids), difficulty swallowing, respiratory paralysis. If respiratory failure: intubate and ventilate, refer to ICU immediately."},
    {"source": "MOHFW_Snakebite_Guidelines_2024", "disease": "Snakebite", "chunk": "REFERRAL CRITERIA FOR SNAKEBITE: Refer to District Hospital if: 1. ASV not available at PHC. 2. Signs of severe envenomation. 3. Respiratory difficulty. 4. Patient needs ventilator support. 5. 20WBCT positive after 2 doses of ASV. WARNING: Do not delay ASV for referral. Give first dose before transport if available."},

    # DENGUE PROTOCOL - Based on WHO/MOHFW Guidelines
    {"source": "MOHFW_Dengue_Guidelines_2024", "disease": "Dengue Fever", "chunk": "DENGUE CLASSIFICATION: Group A (OPD): Fever with 2+ of: headache, retro-orbital pain, myalgia, arthralgia, rash, leukopenia+. No warning signs. Oral fluids, Paracetamol (NOT aspirin/ibuprofen), daily platelet monitoring. Group B (Admit): Warning signs present - abdominal pain, persistent vomiting, fluid accumulation, mucosal bleeding, lethargy, hepatomegaly, rising hematocrit with falling platelets."},
    {"source": "MOHFW_Dengue_Guidelines_2024", "disease": "Dengue Fever", "chunk": "DENGUE GROUP C (Severe - ICU): Severe plasma leakage with shock (DSS), severe bleeding, severe organ impairment (liver AST/ALT >=1000, CNS impairment, heart failure). FLUID MANAGEMENT: Start with NS/RL at 5-7 ml/kg/hr for 1-2 hours. Reduce to 3-5 ml/kg/hr for 2-4 hours. Reduce to 2-3 ml/kg/hr. Adjust based on hematocrit and urine output (0.5 ml/kg/hr target)."},
    {"source": "MOHFW_Dengue_Guidelines_2024", "disease": "Dengue Fever", "chunk": "DENGUE DIAGNOSIS: NS1 Antigen test: Positive in first 5 days of fever. After Day 5: IgM ELISA (current infection), IgG (past/secondary). WARNING SIGNS requiring hospitalization: Abdominal pain or tenderness, persistent vomiting, clinical fluid accumulation, mucosal bleed, lethargy/restlessness, liver enlargement >2cm, lab: increase in HCT concurrent with rapid decrease in platelet count."},
    {"source": "MOHFW_Dengue_Guidelines_2024", "disease": "Dengue Fever", "chunk": "DENGUE: DO NOT GIVE ASPIRIN OR NSAIDS (Ibuprofen, Diclofenac). These increase bleeding risk. Use only PARACETAMOL for fever (10-15 mg/kg/dose every 4-6 hours). Tepid sponging for high fever. Oral rehydration is key. Platelet transfusion only if: active bleeding with platelets <10,000 OR planned procedure with platelets <50,000. Prophylactic platelet transfusion is NOT recommended."},

    # MALARIA PROTOCOL
    {"source": "MOHFW_Malaria_Guidelines_2024", "disease": "Malaria", "chunk": "MALARIA DIAGNOSIS: Use RDT (Rapid Diagnostic Test) or microscopy. RDT detects Pf (P. falciparum) HRP2 and Pv (P. vivax) pLDH. All fever cases in endemic areas should be tested. TREATMENT P. vivax: Chloroquine 25mg/kg over 3 days (Day 1: 10mg/kg, Day 2: 10mg/kg, Day 3: 5mg/kg) + Primaquine 0.25mg/kg daily for 14 days (anti-relapse). Check G6PD before Primaquine."},
    {"source": "MOHFW_Malaria_Guidelines_2024", "disease": "Malaria", "chunk": "SEVERE MALARIA (P. falciparum): Signs: cerebral malaria (altered consciousness), severe anemia (Hb<5g/dl), respiratory distress, hypoglycemia, shock, DIC, renal failure. TREATMENT: Artesunate IV/IM: 2.4 mg/kg at 0, 12, 24 hours, then daily. Minimum 3 doses before switching to oral ACT. If Artesunate unavailable: Quinine IV 20mg/kg loading dose in 5% dextrose over 4 hours, then 10mg/kg every 8 hours."},
    {"source": "MOHFW_Malaria_Guidelines_2024", "disease": "Malaria", "chunk": "MALARIA IN PREGNANCY: All trimesters: ACT (Artemether-Lumefantrine) is safe. Primaquine is CONTRAINDICATED in pregnancy. Chloroquine is safe for P. vivax. Severe malaria in pregnancy: IV Artesunate, same dose as adults. Refer to higher facility. MALARIA IN CHILDREN: Same drugs, adjusted by weight. Watch for hypoglycemia and convulsions."},

    # DIARRHOEAL DISEASE / CHOLERA
    {"source": "MOHFW_Diarrhoea_Guidelines_2024", "disease": "Acute Gastroenteritis", "chunk": "DIARRHOEA MANAGEMENT (WHO/MOHFW): ASSESS DEHYDRATION: Plan A (No dehydration): ORS after every loose stool + Zinc 20mg/day for 14 days + continue feeding. Plan B (Some dehydration): ORS 75ml/kg over 4 hours in health facility. Reassess after 4 hours. Plan C (Severe dehydration): IV RL or NS 100ml/kg. Infants <12 months: 30ml/kg in 1 hour, then 70ml/kg in 5 hours. Older: 30ml/kg in 30 min, then 70ml/kg in 2.5 hours."},
    {"source": "MOHFW_Diarrhoea_Guidelines_2024", "disease": "Acute Gastroenteritis", "chunk": "CHOLERA SUSPECT: If age >2 years with severe watery diarrhoea in known cholera area, OR any patient with severe dehydration from acute watery diarrhoea. TREATMENT: Aggressive fluid replacement (Plan C). Antibiotic: Doxycycline 300mg single dose (adults) or Azithromycin 20mg/kg (children). Zinc supplementation. Report to District Surveillance Officer immediately."},

    # ARI / PNEUMONIA
    {"source": "MOHFW_ARI_IMNCI_Guidelines", "disease": "Pneumonia", "chunk": "PNEUMONIA CLASSIFICATION (IMNCI): NO PNEUMONIA: Cough/cold, no fast breathing, no chest indrawing -> home care, safe remedy for cough. PNEUMONIA: Fast breathing (>50/min for 2-12 months, >40/min for 12-59 months) -> Amoxicillin 40mg/kg/day in 2 divided doses for 5 days. SEVERE PNEUMONIA: Chest indrawing or danger signs -> first dose of Amoxicillin + refer URGENTLY to hospital."},
    {"source": "MOHFW_ARI_IMNCI_Guidelines", "disease": "Pneumonia", "chunk": "DANGER SIGNS IN CHILDREN (IMNCI): Unable to drink/breastfeed, vomits everything, convulsions, lethargic or unconscious, stridor in calm child, severe malnutrition. ANY danger sign -> REFER IMMEDIATELY. Pre-referral: First dose of antibiotic (Amoxicillin or Cotrimoxazole), treat fever (Paracetamol), prevent hypoglycemia (breastfeed or give sugar water), keep warm."},

    # MATERNAL EMERGENCY
    {"source": "MOHFW_EmONC_Guidelines", "disease": "Obstetric Emergency", "chunk": "POST-PARTUM HEMORRHAGE (PPH): Blood loss >500ml after delivery. ACTIVE MANAGEMENT OF THIRD STAGE (AMTSL): 1. Oxytocin 10IU IM within 1 minute of delivery. 2. Controlled cord traction. 3. Uterine massage. IF PPH OCCURS: Oxytocin 20IU in 1L RL at 60 drops/min. Bimanual uterine compression. If not controlled: Misoprostol 800mcg sublingual. REFER immediately if PPH not controlled with uterotonics."},
    {"source": "MOHFW_EmONC_Guidelines", "disease": "Obstetric Emergency", "chunk": "ECLAMPSIA: Convulsions in pregnancy with high BP (>140/90). TREATMENT: Magnesium Sulphate loading dose: 4g IV (20ml of 20% solution) slowly over 5-10 minutes + 5g IM in each buttock (10g total IM). Maintenance: 5g IM every 4 hours in alternate buttock for 24 hours after last seizure. MONITOR: Urine output >30ml/hr, respiratory rate >16/min, knee jerk present. ANTIDOTE for MgSO4 toxicity: Calcium Gluconate 1g (10ml of 10%) IV slowly."},

    # RABIES / DOG BITE
    {"source": "MOHFW_Rabies_Guidelines", "disease": "Rabies", "chunk": "DOG BITE / RABIES PEP (Post-Exposure Prophylaxis): Category I: Touching/feeding animal, licks on intact skin -> No PEP, just wash. Category II: Nibbling, minor scratches/abrasions without bleeding, licks on broken skin -> Wound wash + ARV vaccine (Days 0,3,7,14,28). Category III: Single or multiple transdermal bites or scratches, licks on mucous membrane, exposure to bats -> Wound wash + ARV + Rabies Immunoglobulin (RIG) at wound site."},
    {"source": "MOHFW_Rabies_Guidelines", "disease": "Rabies", "chunk": "RABIES WOUND MANAGEMENT: Immediately wash wound vigorously with soap and running water for 15 minutes. Apply povidone-iodine or alcohol. DO NOT suture wound immediately. Apply RIG infiltrated into and around the wound (for Category III). Dose: 20 IU/kg for human RIG, 40 IU/kg for equine RIG. Remaining RIG inject IM at a site distant from vaccine. Start ARV on Day 0 in deltoid (NOT gluteal)."},
]

with open("c:/Users/gupta/antigravity/new/sanjeevani-rag/data/clinical_protocols.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["chunk_id", "source_document", "disease_category", "chunk_text", "chunk_index"])
    writer.writeheader()
    for i, chunk in enumerate(PROTOCOL_CHUNKS):
        writer.writerow({
            "chunk_id": f"PROTO-{i+1:04d}",
            "source_document": chunk["source"],
            "disease_category": chunk["disease"],
            "chunk_text": chunk["chunk"],
            "chunk_index": i,
        })
print(f"  -> {len(PROTOCOL_CHUNKS)} protocol chunks written")

print("\nDONE! All data files generated in sanjeevani-rag/data/")
print("Files:")
print("  - health_facilities.csv")
print("  - drug_inventory.csv")
print("  - triage_logs.csv")
print("  - clinical_protocols.csv")
