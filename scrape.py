import pandas as pd
import urllib.request
from decimal import Decimal
import json 
import os 

# hospital dimension
dt = pd.read_csv("./dim/hospital.csv")

# concept dimension from OHDSI athena
concept = pd.read_csv("./dim/CONCEPT.csv.gz",compression='gzip',sep="\t")
concept = concept[concept.vocabulary_id=='CPT4']
for index, row in dt.iterrows():
    if row["idn"] == "Tennova Healthcare":
        if row["type"] == "CSV":
            with urllib.request.urlopen(row["file_url"]) as f:
                charges = pd.read_csv(f,skiprows=int(row["skiprow"]),dtype="object",keep_default_na=False) 
            charges[["gross"]] = charges[[row["gross"]]]
            charges[["cash"]] = charges[[row["cash"]]]        
            charges[["concept_code"]] = charges[[row["cpt"]]]        
            charges = charges[["concept_code","gross","cash"]].drop_duplicates().dropna()
            charges["gross"] = charges[["gross"]].apply(lambda x: x.str.replace(",","").str.replace("$",""))
            charges["gross"] = charges["gross"].apply(pd.to_numeric, errors='ignore', downcast='float')
            charges["gross"] = charges["gross"].apply(lambda x: round(Decimal(x),2))
            charges["cash"] = charges[["cash"]].apply(lambda x: x.str.replace(",","").str.replace("$",""))
            charges["cash"] = charges["cash"].apply(pd.to_numeric, errors='ignore', downcast='float')
            charges["cash"] = charges["cash"].apply(lambda x: round(Decimal(x),2))
            charges = pd.merge(charges,concept[["concept_id","concept_code"]],left_on="concept_code",right_on="concept_code")
            charges = charges[["concept_code","cash","gross"]]
            charges = charges.rename(columns={"concept_code":"cpt"})
            charges = charges.sort_values(["cpt"])
            charges.to_json("./" + str(row["hospital_npi"]) + ".jsonl",lines=True,orient="records") 
    if row["idn"] == 'Covenant Health':
        if row["type"] == "JSON":
            os.system('curl ' + row["file_url"] + " | jq > tmp.json")
            with open("tmp.json","r") as f:
                charges = json.load(f)
            charges = charges['data']
            del charges[0]
            code_type = [x[0]['code type'] for x in charges]
            code = [x[0]['code'] for x in charges]
            gross = [x[0]['gross charge'] for x in charges]
            cash = [x[0]['discounted cash price'] for x in charges]
            charges = pd.DataFrame(list(zip(code_type,code,gross,cash)))    
            charges.columns = ['vocabulary_id','concept_code','gross','cash']
            charges["gross"] = charges[["gross"]].apply(lambda x: x.str.replace(",","").str.replace("$",""))
            charges["gross"] = charges["gross"].apply(pd.to_numeric, errors='ignore', downcast='float')
            charges["gross"] = charges["gross"].apply(lambda x: round(Decimal(x),2))
            charges["cash"] = charges[["cash"]].apply(lambda x: x.str.replace(",","").str.replace("$",""))
            charges["cash"] = charges["cash"].apply(pd.to_numeric, errors='ignore', downcast='float')
            charges["cash"] = charges["cash"].apply(lambda x: round(Decimal(x),2))
            charges = charges[charges.vocabulary_id=="cpt"]
            charges = pd.merge(charges,concept[["concept_id","concept_code"]],left_on="concept_code",right_on="concept_code")
            charges = charges[["concept_code","cash","gross"]]
            charges = charges.rename(columns={"concept_code":"cpt"})
            charges = charges.drop_duplicates().sort_values(["cpt"])
            charges.to_json("./" + str(row["hospital_npi"]) + ".jsonl",lines=True,orient="records") 
            if os.path.exists("tmp.json"):
                os.remove("tmp.json")
