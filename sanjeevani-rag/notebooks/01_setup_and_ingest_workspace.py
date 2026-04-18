# Databricks notebook source
# MAGIC %md
# MAGIC # Sanjeevani-RAG: Step 1 - Data Ingestion & Table Creation
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Creates all Delta tables in `vc.sanjeevani` schema
# MAGIC 2. Ingests health facilities, drug inventory, triage logs, and clinical protocols
# MAGIC 3. Adds geohash columns for spatial queries

# COMMAND ----------

# MAGIC %pip install pygeohash
# MAGIC %restart_python

# COMMAND ----------

import pygeohash as pgh
from pyspark.sql.functions import udf, col, lit, when, concat_ws
from pyspark.sql.types import StringType
import csv
import io

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Health Facilities Data

# COMMAND ----------

# Health facilities - Indore Division, MP (44 real facilities)
facilities_data = """facility_id,facility_name,facility_type,state,district,block,latitude,longitude,beds,ownership,phone,operating_hours
MP-IND-DH-001,MY Hospital Indore,DH,Madhya Pradesh,Indore,Indore Urban,22.7196,75.8577,500,Government,0731-2528439,24x7
MP-IND-DH-002,CHL Hospital Indore,DH,Madhya Pradesh,Indore,Indore Urban,22.7241,75.8839,200,Government,0731-2734164,24x7
MP-DEW-DH-003,District Hospital Dewas,DH,Madhya Pradesh,Dewas,Dewas,22.9623,76.0508,150,Government,0731-2481847,24x7
MP-UJJ-DH-004,District Hospital Ujjain,DH,Madhya Pradesh,Ujjain,Ujjain,23.1765,75.7885,200,Government,0731-2919498,24x7
MP-DHA-DH-005,District Hospital Dhar,DH,Madhya Pradesh,Dhar,Dhar,22.5972,75.2997,120,Government,0731-2218498,24x7
MP-KHA-DH-006,District Hospital Khandwa,DH,Madhya Pradesh,Khandwa,Khandwa,21.8243,76.3483,130,Government,0731-2135700,24x7
MP-KHA-DH-007,District Hospital Khargone,DH,Madhya Pradesh,Khargone,Khargone,21.8242,75.6145,100,Government,0731-2651498,24x7
MP-BUR-DH-008,District Hospital Burhanpur,DH,Madhya Pradesh,Burhanpur,Burhanpur,21.3092,76.2256,80,Government,0731-2968039,24x7
MP-IND-CHC-009,CHC Mhow,CHC,Madhya Pradesh,Indore,Mhow,22.5548,75.7633,30,Government,0731-2756607,24x7
MP-IND-CHC-010,CHC Sanwer,CHC,Madhya Pradesh,Indore,Sanwer,22.9714,75.8266,30,Government,0731-2960907,24x7
MP-IND-CHC-011,CHC Depalpur,CHC,Madhya Pradesh,Indore,Depalpur,22.8547,75.5366,30,Government,0731-2281900,24x7
MP-IND-CHC-012,CHC Hatod,CHC,Madhya Pradesh,Indore,Hatod,22.7733,75.735,20,Government,0731-2268230,24x7
MP-IND-CHC-013,CHC Manpur,CHC,Madhya Pradesh,Indore,Manpur,22.4343,75.62,20,Government,0731-2145457,24x7
MP-DEW-CHC-014,CHC Sonkatch,CHC,Madhya Pradesh,Dewas,Sonkatch,22.9761,76.3608,30,Government,0731-2093291,24x7
MP-DEW-CHC-015,CHC Khategaon,CHC,Madhya Pradesh,Dewas,Khategaon,22.5966,76.9107,25,Government,0731-2439653,24x7
MP-DEW-CHC-016,CHC Bagli,CHC,Madhya Pradesh,Dewas,Bagli,22.6358,76.3454,20,Government,0731-2653668,24x7
MP-UJJ-CHC-017,CHC Mahidpur,CHC,Madhya Pradesh,Ujjain,Mahidpur,23.4785,75.6517,30,Government,0731-2261543,24x7
MP-UJJ-CHC-018,CHC Tarana,CHC,Madhya Pradesh,Ujjain,Tarana,23.3281,76.0317,25,Government,0731-2938568,24x7
MP-DHA-CHC-019,CHC Badnawar,CHC,Madhya Pradesh,Dhar,Badnawar,23.021,75.6261,25,Government,0731-2757043,24x7
MP-DHA-CHC-020,CHC Dharampuri,CHC,Madhya Pradesh,Dhar,Dharampuri,22.1491,75.344,20,Government,0731-2131810,24x7
MP-BUR-CHC-021,CHC Nepanagar,CHC,Madhya Pradesh,Burhanpur,Nepanagar,21.4571,76.3919,20,Government,0731-2965063,24x7
MP-IND-PHC-022,PHC Rau,PHC,Madhya Pradesh,Indore,Indore Urban,22.677,75.8103,6,Government,0731-2103862,8AM-5PM
MP-IND-PHC-023,PHC Betma,PHC,Madhya Pradesh,Indore,Mhow,22.6839,75.614,6,Government,0731-2685498,8AM-5PM
MP-IND-PHC-024,PHC Simrol,PHC,Madhya Pradesh,Indore,Mhow,22.5391,75.8947,6,Government,0731-2476413,8AM-5PM
MP-IND-PHC-025,PHC Gautampura,PHC,Madhya Pradesh,Indore,Depalpur,22.8643,75.4955,6,Government,0731-2050966,8AM-5PM
MP-IND-PHC-026,PHC Kampel,PHC,Madhya Pradesh,Indore,Sanwer,22.899,75.8985,6,Government,0731-2989652,8AM-5PM
MP-IND-PHC-027,PHC Manasa,PHC,Madhya Pradesh,Indore,Manpur,22.415,75.58,4,Government,0731-2849698,8AM-5PM
MP-DEW-PHC-028,PHC Tonk Khurd,PHC,Madhya Pradesh,Dewas,Dewas,22.885,76.12,6,Government,0731-2362997,8AM-5PM
MP-DEW-PHC-029,PHC Kannod,PHC,Madhya Pradesh,Dewas,Kannod,22.67,76.7433,6,Government,0731-2820032,8AM-5PM
MP-UJJ-PHC-030,PHC Nagda,PHC,Madhya Pradesh,Ujjain,Nagda,23.4572,75.4155,6,Government,0731-2413439,8AM-5PM
MP-UJJ-PHC-031,PHC Barnagar,PHC,Madhya Pradesh,Ujjain,Barnagar,23.049,75.5683,6,Government,0731-2927050,8AM-5PM
MP-DHA-PHC-032,PHC Sardarpur,PHC,Madhya Pradesh,Dhar,Sardarpur,22.69,74.89,4,Government,0731-2137247,8AM-5PM
MP-DHA-PHC-033,PHC Manawar,PHC,Madhya Pradesh,Dhar,Manawar,22.235,75.0885,4,Government,0731-2773963,8AM-5PM
MP-KHA-PHC-034,PHC Pandhana,PHC,Madhya Pradesh,Khandwa,Pandhana,21.6853,76.2072,6,Government,0731-2471889,8AM-5PM
MP-KHA-PHC-035,PHC Punasa,PHC,Madhya Pradesh,Khandwa,Punasa,22.2369,76.376,4,Government,0731-2416975,8AM-5PM
MP-KHA-PHC-036,PHC Kasrawad,PHC,Madhya Pradesh,Khargone,Kasrawad,21.92,75.5947,4,Government,0731-2484614,8AM-5PM
MP-KHA-PHC-037,PHC Bhikangaon,PHC,Madhya Pradesh,Khargone,Bhikangaon,21.5555,75.9505,4,Government,0731-2456802,8AM-5PM
MP-IND-SC-038,SC Limbodi,SC,Madhya Pradesh,Indore,Sanwer,22.935,75.795,0,Government,0731-2768987,8AM-5PM
MP-IND-SC-039,SC Palda,SC,Madhya Pradesh,Indore,Indore Urban,22.752,75.823,0,Government,0731-2474988,8AM-5PM
MP-IND-SC-040,SC Pipliyahana,SC,Madhya Pradesh,Indore,Indore Urban,22.7398,75.8655,0,Government,0731-2612651,8AM-5PM
MP-IND-SC-041,SC Khajrana,SC,Madhya Pradesh,Indore,Indore Urban,22.715,75.902,0,Government,0731-2076025,8AM-5PM
MP-IND-SC-042,SC Rangwasa,SC,Madhya Pradesh,Indore,Mhow,22.58,75.71,0,Government,0731-2553973,8AM-5PM
MP-DEW-SC-043,SC Tejpur,SC,Madhya Pradesh,Dewas,Dewas,22.93,76.08,0,Government,0731-2044270,8AM-5PM
MP-UJJ-SC-044,SC Unhel,SC,Madhya Pradesh,Ujjain,Ujjain,23.21,75.82,0,Government,0731-2597710,8AM-5PM"""

# Parse and create DataFrame
rows = []
reader = csv.DictReader(io.StringIO(facilities_data))
for row in reader:
    row['latitude'] = float(row['latitude'])
    row['longitude'] = float(row['longitude'])
    row['beds'] = int(row['beds'])
    rows.append(row)

df_facilities = spark.createDataFrame(rows)

# Add geohash
geohash_udf = udf(lambda lat, lng: pgh.encode(lat, lng, precision=6), StringType())
df_facilities = df_facilities.withColumn("geohash", geohash_udf(col("latitude"), col("longitude")))

# Write to Unity Catalog
df_facilities.write.mode("overwrite").saveAsTable("vc.sanjeevani.health_facilities")
print(f"Health facilities table created: {df_facilities.count()} rows")
display(df_facilities)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Drug Inventory Data

# COMMAND ----------

# Read the drug inventory CSV (embedded inline - too large, using file upload approach)
# For brevity, we generate it directly in Spark

import random
from datetime import datetime, timedelta
from pyspark.sql import Row

random.seed(42)

DRUGS = [
    ("Polyvalent Anti-Snake Venom (ASV)", "Antivenom", "vials", "P", True),
    ("Artesunate Injection 60mg", "Antimalarial", "vials", "P", True),
    ("Artemether-Lumefantrine Tablets", "Antimalarial", "tablets", "P", True),
    ("Chloroquine Phosphate 250mg", "Antimalarial", "tablets", "P", False),
    ("Primaquine 7.5mg", "Antimalarial", "tablets", "P", False),
    ("Dengue NS1 Antigen Test Kit", "Diagnostic", "kits", "P", True),
    ("Platelet Rich Plasma", "Blood Product", "units", "S", True),
    ("Normal Saline 0.9% (1L)", "IV Fluid", "bottles", "P", True),
    ("Ringer Lactate (1L)", "IV Fluid", "bottles", "P", True),
    ("ORS Packets (WHO formula)", "ORS", "packets", "P", True),
    ("Zinc Sulphate Dispersible 20mg", "Micronutrient", "tablets", "P", False),
    ("Metronidazole 400mg", "Antiprotozoal", "tablets", "P", False),
    ("Amoxicillin 500mg", "Antibiotic", "capsules", "P", False),
    ("Azithromycin 500mg", "Antibiotic", "tablets", "P", False),
    ("Ciprofloxacin 500mg", "Antibiotic", "tablets", "P", False),
    ("Cotrimoxazole Paediatric", "Antibiotic", "tablets", "P", False),
    ("Gentamicin Injection 80mg", "Antibiotic", "ampoules", "P", True),
    ("Ceftriaxone Injection 1g", "Antibiotic", "vials", "S", True),
    ("Oxytocin Injection 5IU", "Obstetric", "ampoules", "P", True),
    ("Misoprostol 200mcg", "Obstetric", "tablets", "P", True),
    ("Magnesium Sulphate Injection 50%", "Anticonvulsant", "ampoules", "P", True),
    ("Adrenaline (Epinephrine) 1mg/ml", "Emergency", "ampoules", "P", True),
    ("Hydrocortisone Injection 100mg", "Corticosteroid", "vials", "P", True),
    ("Atropine Sulphate 0.6mg/ml", "Anticholinergic", "ampoules", "P", True),
    ("Paracetamol 500mg", "Analgesic", "tablets", "P", False),
    ("Ibuprofen 400mg", "NSAID", "tablets", "P", False),
    ("Diclofenac Sodium 50mg", "NSAID", "tablets", "P", False),
    ("Salbutamol Inhaler 100mcg", "Bronchodilator", "inhalers", "P", False),
    ("Dexamethasone Injection 4mg", "Corticosteroid", "ampoules", "P", True),
    ("Prednisolone 5mg", "Corticosteroid", "tablets", "P", False),
    ("Metformin 500mg", "Antidiabetic", "tablets", "P", False),
    ("Glimepiride 1mg", "Antidiabetic", "tablets", "P", False),
    ("Amlodipine 5mg", "Antihypertensive", "tablets", "P", False),
    ("Enalapril 5mg", "Antihypertensive", "tablets", "P", False),
    ("Tetanus Toxoid Injection", "Vaccine", "doses", "P", True),
    ("Anti-Tetanus Serum (ATS)", "Antiserum", "vials", "P", True),
    ("Anti-Rabies Vaccine (ARV)", "Vaccine", "doses", "P", True),
    ("Rabies Immunoglobulin (RIG)", "Immunoglobulin", "vials", "S", True),
    ("Activated Charcoal Powder", "Antidote", "sachets", "P", True),
    ("Pralidoxime (PAM) Injection", "Antidote", "vials", "S", True),
]

# Get facility list with all necessary columns (consistent with Cell 11)
facilities_pdf = df_facilities.select("facility_id", "facility_type").toPandas()

inventory_rows = []
for _, fac in facilities_pdf.iterrows():
    for drug_name, category, unit, nlem, critical in DRUGS:
        ftype = fac['facility_type']
        # Skip inappropriate drugs per facility level
        if ftype == "SC" and nlem != "P":
            continue
        if ftype == "SC" and category in ("Blood Product", "Immunoglobulin", "Antidote"):
            continue
        if ftype == "PHC" and nlem == "S":
            continue
        
        max_stock = {"DH": 200, "CHC": 80, "PHC": 30, "SC": 10}.get(ftype, 10)
        if critical:
            max_stock = max_stock // 2
        
        oos_chance = {"SC": 0.25, "PHC": 0.15, "CHC": 0.05, "DH": 0.02}.get(ftype, 0.1)
        
        if critical and random.random() < oos_chance:
            qty = 0
            status = "OUT_OF_STOCK"
        elif random.random() < 0.10:
            qty = random.randint(1, max(1, max_stock // 10))
            status = "LOW"
        else:
            qty = random.randint(max(1, max_stock // 3), max_stock)
            status = "IN_STOCK"
        
        restock_date = (datetime.now() - timedelta(days=random.randint(1, 60))).strftime("%Y-%m-%d")
        
        inventory_rows.append(Row(
            facility_id=fac['facility_id'],
            drug_name=drug_name,
            drug_category=category,
            nlem_level=nlem,
            quantity_available=qty,
            unit=unit,
            last_restocked=restock_date,
            stock_status=status,
            critical_flag=critical
        ))

df_inventory = spark.createDataFrame(inventory_rows)
df_inventory.write.mode("overwrite").saveAsTable("vc.sanjeevani.drug_inventory")
print(f"Drug inventory table created: {df_inventory.count()} rows")

# Show some critical OUT_OF_STOCK items
display(df_inventory.filter(col("stock_status") == "OUT_OF_STOCK").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Clinical Protocols

# COMMAND ----------

protocol_chunks = [
    ("MOHFW_Snakebite_Guidelines_2024", "Snakebite", "SNAKEBITE FIRST AID: 1. Reassure and immobilize the patient. Keep the bitten limb below heart level. 2. Do NOT apply tourniquet, do NOT cut the wound, do NOT suck the venom. 3. Remove rings, watches, tight clothing from bitten limb. 4. Transport patient to nearest health facility with Anti-Snake Venom (ASV) immediately. 5. Note the time of bite and description of snake if possible."),
    ("MOHFW_Snakebite_Guidelines_2024", "Snakebite", "ASV ADMINISTRATION: Polyvalent Anti-Snake Venom is effective against Big Four: Cobra, Krait, Russell's Viper, Saw-scaled Viper. Dose: Start with 10 vials (100ml) ASV diluted in Normal Saline, IV infusion over 1 hour. Premedicate with Adrenaline 0.25mg SC, Hydrocortisone 200mg IV, and Chlorpheniramine 10mg IV. Watch for anaphylaxis. If signs of envenomation persist after 1-2 hours, repeat 10 vials."),
    ("MOHFW_Snakebite_Guidelines_2024", "Snakebite", "SIGNS OF ENVENOMATION - HEMOTOXIC (Viper): Local swelling progressing beyond bite site, bleeding from gums/injection sites, blood in urine, blood not clotting (20 Minute Whole Blood Clotting Test - 20WBCT). NEUROTOXIC (Krait/Cobra): Ptosis (drooping eyelids), difficulty swallowing, respiratory paralysis. If respiratory failure: intubate and ventilate, refer to ICU immediately."),
    ("MOHFW_Snakebite_Guidelines_2024", "Snakebite", "REFERRAL CRITERIA FOR SNAKEBITE: Refer to District Hospital if: 1. ASV not available at PHC. 2. Signs of severe envenomation. 3. Respiratory difficulty. 4. Patient needs ventilator support. 5. 20WBCT positive after 2 doses of ASV. WARNING: Do not delay ASV for referral. Give first dose before transport if available."),
    ("MOHFW_Dengue_Guidelines_2024", "Dengue Fever", "DENGUE CLASSIFICATION: Group A (OPD): Fever with 2+ of: headache, retro-orbital pain, myalgia, arthralgia, rash, leukopenia+. No warning signs. Oral fluids, Paracetamol (NOT aspirin/ibuprofen), daily platelet monitoring. Group B (Admit): Warning signs present - abdominal pain, persistent vomiting, fluid accumulation, mucosal bleeding, lethargy, hepatomegaly, rising hematocrit with falling platelets."),
    ("MOHFW_Dengue_Guidelines_2024", "Dengue Fever", "DENGUE GROUP C (Severe - ICU): Severe plasma leakage with shock (DSS), severe bleeding, severe organ impairment (liver AST/ALT >=1000, CNS impairment, heart failure). FLUID MANAGEMENT: Start with NS/RL at 5-7 ml/kg/hr for 1-2 hours. Reduce to 3-5 ml/kg/hr for 2-4 hours. Reduce to 2-3 ml/kg/hr. Adjust based on hematocrit and urine output (0.5 ml/kg/hr target)."),
    ("MOHFW_Dengue_Guidelines_2024", "Dengue Fever", "DENGUE DIAGNOSIS: NS1 Antigen test: Positive in first 5 days of fever. After Day 5: IgM ELISA (current infection), IgG (past/secondary). WARNING SIGNS requiring hospitalization: Abdominal pain or tenderness, persistent vomiting, clinical fluid accumulation, mucosal bleed, lethargy/restlessness, liver enlargement >2cm, lab: increase in HCT concurrent with rapid decrease in platelet count."),
    ("MOHFW_Dengue_Guidelines_2024", "Dengue Fever", "DENGUE: DO NOT GIVE ASPIRIN OR NSAIDS (Ibuprofen, Diclofenac). These increase bleeding risk. Use only PARACETAMOL for fever (10-15 mg/kg/dose every 4-6 hours). Tepid sponging for high fever. Oral rehydration is key. Platelet transfusion only if: active bleeding with platelets <10,000 OR planned procedure with platelets <50,000. Prophylactic platelet transfusion is NOT recommended."),
    ("MOHFW_Malaria_Guidelines_2024", "Malaria", "MALARIA DIAGNOSIS: Use RDT (Rapid Diagnostic Test) or microscopy. RDT detects Pf (P. falciparum) HRP2 and Pv (P. vivax) pLDH. All fever cases in endemic areas should be tested. TREATMENT P. vivax: Chloroquine 25mg/kg over 3 days (Day 1: 10mg/kg, Day 2: 10mg/kg, Day 3: 5mg/kg) + Primaquine 0.25mg/kg daily for 14 days (anti-relapse). Check G6PD before Primaquine."),
    ("MOHFW_Malaria_Guidelines_2024", "Malaria", "SEVERE MALARIA (P. falciparum): Signs: cerebral malaria (altered consciousness), severe anemia (Hb<5g/dl), respiratory distress, hypoglycemia, shock, DIC, renal failure. TREATMENT: Artesunate IV/IM: 2.4 mg/kg at 0, 12, 24 hours, then daily. Minimum 3 doses before switching to oral ACT. If Artesunate unavailable: Quinine IV 20mg/kg loading dose in 5% dextrose over 4 hours, then 10mg/kg every 8 hours."),
    ("MOHFW_Malaria_Guidelines_2024", "Malaria", "MALARIA IN PREGNANCY: All trimesters: ACT (Artemether-Lumefantrine) is safe. Primaquine is CONTRAINDICATED in pregnancy. Chloroquine is safe for P. vivax. Severe malaria in pregnancy: IV Artesunate, same dose as adults. Refer to higher facility. MALARIA IN CHILDREN: Same drugs, adjusted by weight. Watch for hypoglycemia and convulsions."),
    ("MOHFW_Diarrhoea_Guidelines_2024", "Acute Gastroenteritis", "DIARRHOEA MANAGEMENT (WHO/MOHFW): ASSESS DEHYDRATION: Plan A (No dehydration): ORS after every loose stool + Zinc 20mg/day for 14 days + continue feeding. Plan B (Some dehydration): ORS 75ml/kg over 4 hours in health facility. Reassess after 4 hours. Plan C (Severe dehydration): IV RL or NS 100ml/kg. Infants <12 months: 30ml/kg in 1 hour, then 70ml/kg in 5 hours. Older: 30ml/kg in 30 min, then 70ml/kg in 2.5 hours."),
    ("MOHFW_Diarrhoea_Guidelines_2024", "Acute Gastroenteritis", "CHOLERA SUSPECT: If age >2 years with severe watery diarrhoea in known cholera area, OR any patient with severe dehydration from acute watery diarrhoea. TREATMENT: Aggressive fluid replacement (Plan C). Antibiotic: Doxycycline 300mg single dose (adults) or Azithromycin 20mg/kg (children). Zinc supplementation. Report to District Surveillance Officer immediately."),
    ("MOHFW_ARI_IMNCI_Guidelines", "Pneumonia", "PNEUMONIA CLASSIFICATION (IMNCI): NO PNEUMONIA: Cough/cold, no fast breathing, no chest indrawing - home care, safe remedy for cough. PNEUMONIA: Fast breathing (>50/min for 2-12 months, >40/min for 12-59 months) - Amoxicillin 40mg/kg/day in 2 divided doses for 5 days. SEVERE PNEUMONIA: Chest indrawing or danger signs - first dose of Amoxicillin + refer URGENTLY to hospital."),
    ("MOHFW_ARI_IMNCI_Guidelines", "Pneumonia", "DANGER SIGNS IN CHILDREN (IMNCI): Unable to drink/breastfeed, vomits everything, convulsions, lethargic or unconscious, stridor in calm child, severe malnutrition. ANY danger sign - REFER IMMEDIATELY. Pre-referral: First dose of antibiotic (Amoxicillin or Cotrimoxazole), treat fever (Paracetamol), prevent hypoglycemia (breastfeed or give sugar water), keep warm."),
    ("MOHFW_EmONC_Guidelines", "Obstetric Emergency", "POST-PARTUM HEMORRHAGE (PPH): Blood loss >500ml after delivery. ACTIVE MANAGEMENT OF THIRD STAGE (AMTSL): 1. Oxytocin 10IU IM within 1 minute of delivery. 2. Controlled cord traction. 3. Uterine massage. IF PPH OCCURS: Oxytocin 20IU in 1L RL at 60 drops/min. Bimanual uterine compression. If not controlled: Misoprostol 800mcg sublingual. REFER immediately if PPH not controlled with uterotonics."),
    ("MOHFW_EmONC_Guidelines", "Obstetric Emergency", "ECLAMPSIA: Convulsions in pregnancy with high BP (>140/90). TREATMENT: Magnesium Sulphate loading dose: 4g IV (20ml of 20% solution) slowly over 5-10 minutes + 5g IM in each buttock (10g total IM). Maintenance: 5g IM every 4 hours in alternate buttock for 24 hours after last seizure. MONITOR: Urine output >30ml/hr, respiratory rate >16/min, knee jerk present. ANTIDOTE for MgSO4 toxicity: Calcium Gluconate 1g (10ml of 10%) IV slowly."),
    ("MOHFW_Rabies_Guidelines", "Rabies", "DOG BITE / RABIES PEP (Post-Exposure Prophylaxis): Category I: Touching/feeding animal, licks on intact skin - No PEP, just wash. Category II: Nibbling, minor scratches/abrasions without bleeding, licks on broken skin - Wound wash + ARV vaccine (Days 0,3,7,14,28). Category III: Single or multiple transdermal bites or scratches, licks on mucous membrane, exposure to bats - Wound wash + ARV + Rabies Immunoglobulin (RIG) at wound site."),
    ("MOHFW_Rabies_Guidelines", "Rabies", "RABIES WOUND MANAGEMENT: Immediately wash wound vigorously with soap and running water for 15 minutes. Apply povidone-iodine or alcohol. DO NOT suture wound immediately. Apply RIG infiltrated into and around the wound (for Category III). Dose: 20 IU/kg for human RIG, 40 IU/kg for equine RIG. Remaining RIG inject IM at a site distant from vaccine. Start ARV on Day 0 in deltoid (NOT gluteal)."),
]

protocol_rows = [Row(chunk_id=f"PROTO-{i+1:04d}", source_document=src, disease_category=disease, chunk_text=chunk, chunk_index=i) for i, (src, disease, chunk) in enumerate(protocol_chunks)]

df_protocols = spark.createDataFrame(protocol_rows)
df_protocols.write.mode("overwrite").saveAsTable("vc.sanjeevani.clinical_protocols")
print(f"Clinical protocols table created: {df_protocols.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Triage Logs (seed data with outbreak clusters)

# COMMAND ----------

import uuid

random.seed(42)
now = datetime.now()

ASHA_WORKERS = [f"ASHA-{d[:3].upper()}-{j:03d}" for d in ["Indore", "Dewas", "Ujjain", "Dhar", "Khandwa", "Khargone", "Burhanpur"] for j in range(1, 16)]

SYMPTOM_CLUSTERS = {
    "dengue_like": {
        "symptoms": ["high fever", "severe headache", "joint pain", "muscle pain", "rash", "retro-orbital pain", "nausea", "vomiting"],
        "hindi": ["tez bukhar", "sir me dard", "jodon me dard", "shareer me dard", "lal daane", "ulti", "ji machlana"],
        "disease": "Dengue Fever"
    },
    "malaria_like": {
        "symptoms": ["intermittent fever", "chills", "rigors", "sweating", "headache", "body ache", "nausea"],
        "hindi": ["thandi lagna", "kaanpna", "pasina", "bukhar aata jaata", "badan dard"],
        "disease": "Malaria"
    },
    "gastro_acute": {
        "symptoms": ["watery diarrhea", "vomiting", "abdominal cramps", "dehydration", "fever"],
        "hindi": ["dast", "ulti", "pet me dard", "paani ki kami", "bukhar"],
        "disease": "Acute Gastroenteritis"
    },
    "snakebite": {
        "symptoms": ["bite mark", "swelling at bite site", "pain", "bleeding from gums", "blurred vision", "difficulty breathing"],
        "hindi": ["saanp ne kaata", "soojan", "dard", "masoodhe se khoon", "dhundla dikhna", "saans me taklif"],
        "disease": "Snakebite"
    },
    "ari_pneumonia": {
        "symptoms": ["cough", "rapid breathing", "chest indrawing", "fever", "difficulty breathing", "wheezing"],
        "hindi": ["khansi", "tez saans", "chhati dhansna", "bukhar", "saans me taklif"],
        "disease": "Pneumonia"
    },
    "maternal_emergency": {
        "symptoms": ["heavy bleeding", "severe abdominal pain", "convulsions", "high BP", "headache", "swelling face hands"],
        "hindi": ["bahut zyada khoon", "pet me bahut dard", "daurey", "BP badha hua", "sir dard", "chehra haath sooja hua"],
        "disease": "Obstetric Emergency"
    },
    "scrub_typhus": {
        "symptoms": ["fever", "eschar", "maculopapular rash", "headache", "muscle pain", "swollen lymph nodes"],
        "hindi": ["bukhar", "kaala nishan", "lal daane", "sir dard", "maanspeshi me dard", "gilti"],
        "disease": "Scrub Typhus"
    },
    "heatstroke": {
        "symptoms": ["high body temperature", "altered mental state", "hot dry skin", "nausea", "rapid breathing", "rapid pulse"],
        "hindi": ["bahut garam shareer", "behosh", "garam sookhi chamdi", "ulti", "tez saans", "tez dhadkan"],
        "disease": "Heatstroke"
    },
}

triage_rows = []

# DENGUE OUTBREAK cluster near Mhow (22.555, 75.763)
for k in range(75):
    ts = now - timedelta(hours=random.randint(2, 44))
    ci = SYMPTOM_CLUSTERS["dengue_like"]
    ns = random.randint(3, 5)
    symp = random.sample(ci["symptoms"], ns)
    hindi = random.sample(ci["hindi"], min(ns, len(ci["hindi"])))
    triage_rows.append(Row(
        log_id=str(uuid.uuid4()), timestamp=ts.strftime("%Y-%m-%d %H:%M:%S"),
        asha_worker_id=random.choice([a for a in ASHA_WORKERS if "IND" in a]),
        patient_age=random.randint(5,65), patient_gender=random.choice(["M","F"]),
        symptoms_raw=", ".join(hindi), symptoms_extracted=", ".join(symp),
        symptom_cluster="dengue_like",
        latitude=round(22.555 + random.gauss(0, 0.03), 6),
        longitude=round(75.763 + random.gauss(0, 0.03), 6),
        district="Indore", severity_score=random.randint(3,5)
    ))

# GASTRO cluster near Dewas (22.96, 76.05)
for k in range(50):
    ts = now - timedelta(hours=random.randint(4, 40))
    ci = SYMPTOM_CLUSTERS["gastro_acute"]
    ns = random.randint(3, 4)
    symp = random.sample(ci["symptoms"], ns)
    hindi = random.sample(ci["hindi"], min(ns, len(ci["hindi"])))
    triage_rows.append(Row(
        log_id=str(uuid.uuid4()), timestamp=ts.strftime("%Y-%m-%d %H:%M:%S"),
        asha_worker_id=random.choice([a for a in ASHA_WORKERS if "DEW" in a]),
        patient_age=random.randint(2,50), patient_gender=random.choice(["M","F"]),
        symptoms_raw=", ".join(hindi), symptoms_extracted=", ".join(symp),
        symptom_cluster="gastro_acute",
        latitude=round(22.96 + random.gauss(0, 0.02), 6),
        longitude=round(76.05 + random.gauss(0, 0.02), 6),
        district="Dewas", severity_score=random.randint(2,4)
    ))

# Background noise (500 scattered cases, 7 days)
weights = [0.15, 0.15, 0.15, 0.10, 0.15, 0.10, 0.10, 0.10]
cluster_names = list(SYMPTOM_CLUSTERS.keys())
# Get facility list with all required columns
facilities_pdf = df_facilities.select("facility_id", "facility_type", "latitude", "longitude", "district").toPandas()
fac_list = facilities_pdf.to_dict('records')

for k in range(500):
    cn = random.choices(cluster_names, weights=weights)[0]
    ci = SYMPTOM_CLUSTERS[cn]
    ts = now - timedelta(hours=random.randint(1, 168))
    fac = random.choice(fac_list)
    ns = random.randint(2, 4)
    symp = random.sample(ci["symptoms"], min(ns, len(ci["symptoms"])))
    hindi = random.sample(ci["hindi"], min(ns, len(ci["hindi"])))
    triage_rows.append(Row(
        log_id=str(uuid.uuid4()), timestamp=ts.strftime("%Y-%m-%d %H:%M:%S"),
        asha_worker_id=random.choice(ASHA_WORKERS),
        patient_age=random.randint(1,80), patient_gender=random.choice(["M","F"]),
        symptoms_raw=", ".join(hindi), symptoms_extracted=", ".join(symp),
        symptom_cluster=cn,
        latitude=round(float(fac['latitude']) + random.gauss(0, 0.05), 6),
        longitude=round(float(fac['longitude']) + random.gauss(0, 0.05), 6),
        district=fac['district'], severity_score=random.randint(1,5)
    ))

df_triage = spark.createDataFrame(triage_rows)
df_triage.write.mode("overwrite").saveAsTable("vc.sanjeevani.triage_logs")
print(f"Triage logs table created: {df_triage.count()} rows")

# COMMAND ----------

# Create empty outbreak_alerts table
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

outbreak_schema = StructType([
    StructField("alert_id", StringType()),
    StructField("detected_at", StringType()),
    StructField("disease_suspected", StringType()),
    StructField("symptom_cluster", StringType()),
    StructField("centroid_lat", DoubleType()),
    StructField("centroid_lng", DoubleType()),
    StructField("radius_km", DoubleType()),
    StructField("case_count", IntegerType()),
    StructField("severity", StringType()),
    StructField("affected_districts", StringType()),
    StructField("status", StringType()),
])

df_empty = spark.createDataFrame([], outbreak_schema)
df_empty.write.mode("overwrite").saveAsTable("vc.sanjeevani.outbreak_alerts")
print("Outbreak alerts table created (empty)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify All Tables

# COMMAND ----------

for table in ["health_facilities", "drug_inventory", "clinical_protocols", "triage_logs", "outbreak_alerts"]:
    count = spark.table(f"vc.sanjeevani.{table}").count()
    print(f"vc.sanjeevani.{table}: {count} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick Data Quality Checks

# COMMAND ----------

# Show facilities out of stock for critical drugs
print("=== CRITICAL DRUG SHORTAGES ===")
display(
    spark.sql("""
        SELECT i.facility_id, f.facility_name, f.facility_type, f.district,
               i.drug_name, i.drug_category, i.stock_status
        FROM vc.sanjeevani.drug_inventory i
        JOIN vc.sanjeevani.health_facilities f ON i.facility_id = f.facility_id
        WHERE i.stock_status = 'OUT_OF_STOCK' AND i.critical_flag = true
        ORDER BY f.district, f.facility_type
    """)
)
