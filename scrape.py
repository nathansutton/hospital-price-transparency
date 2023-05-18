import pandas as pd
import urllib.request
from decimal import Decimal
import json 
import os 
from datetime import date

def cleanup_charges(charges: pd.DataFrame, rename: bool, gross: str, cash: str, cpt: str) -> pd.DataFrame:
    """standarize the cleaning into a normalized table of prices"""
    if rename:
        charges["gross"] = charges[gross]
        charges["cash"] = charges[cash]        
        charges["concept_code"] = charges[[cpt]]
        charges["concept_code"] = charges["concept_code"].apply(lambda x: strip_zero(x.strip()))
        charges["vocabulary_id"] = "cpt"

    if charges.gross.dtype != 'float64':
        charges["gross"] = charges[["gross"]].apply(lambda x: x.str.replace(",","").str.replace("$",""))
        charges["gross"] = charges["gross"].apply(pd.to_numeric, errors='ignore', downcast='float')
    if charges.cash.dtype != 'float64':
        charges["cash"] = charges[["cash"]].apply(lambda x: x.str.replace(",","").str.replace("$",""))
        charges["cash"] = charges["cash"].apply(pd.to_numeric, errors='ignore', downcast='float')


    charges = charges[charges.vocabulary_id == "cpt"]
    charges = pd.merge(charges,concept[["concept_code"]],left_on="concept_code",right_on="concept_code")
    charges = charges.groupby(["vocabulary_id","concept_code"])[["cash","gross"]].max().reset_index()
    charges = pd.melt(charges,id_vars="concept_code",value_vars=["cash","gross"])
    charges = charges.rename(columns={"concept_code":"cpt","variable":"type","value":"price"})
    charges = charges.drop_duplicates().dropna().round(2).sort_values(["cpt","type"])
    return charges


def load_json(url: str) -> dict:
    """requests gets a 403 forbidden a lot, so just use curl"""
    os.system('curl ' + url + " | jq > tmp.json")
    with open("tmp.json","r") as f:
        charges = json.load(f)
    if os.path.exists("tmp.json"):
        os.remove("tmp.json")                
    return charges

def strip_zero(string: str) -> str:
    """some unfortunate individuals pad their cpt codes with zeros"""
    if len(string) == 6 and string[0] == "0":
        return string[1:]
    else: 
        return string


# hospital dimension
dt = pd.read_csv("./dim/hospital.csv")
dt = dt[dt.can_automate == True] # only include those that are working based on a control flag

# concept dimension from OHDSI athena
concept = pd.read_csv("./dim/CONCEPT.csv.gz",compression='gzip',sep="\t")
concept = concept[(concept.vocabulary_id=='CPT4')] # there are technically overlaps in this code set, improve in the future

# status 
status = []

for index, row in dt.iterrows():

    try:

        if row["idn"] == "Parkridge":
            with urllib.request.urlopen(row["file_url"]) as f:
                charges = pd.read_csv(f,skiprows=int(row["skiprow"]),dtype="object",keep_default_na=False) 
            charges = cleanup_charges(
                charges = charges,
                rename = True,
                gross = row["gross"],
                cash = row["cash"],
                cpt = row["cpt"]
            )
            charges.to_json("./data/" + str(row["hospital_npi"]) + ".jsonl",lines=True,orient="records") 


        if row["idn"] == "Mission Health":
            with urllib.request.urlopen(row["file_url"]) as f:
                charges = pd.read_csv(f,skiprows=int(row["skiprow"]),dtype="object",keep_default_na=False) 
            charges = cleanup_charges(
                charges = charges,
                rename = True,
                gross = row["gross"],
                cash = row["cash"],
                cpt = row["cpt"]
            )
            charges.to_json("./data/" + str(row["hospital_npi"]) + ".jsonl",lines=True,orient="records") 
            

        if row["idn"] == "Advent Health":
            charges = load_json(row["file_url"])
            code_type = ['cpt' if 'CPT' in x['Code Type'] else 'ot' for x in charges[0]]
            cpt = [x['Code'] for x in charges[0]]
            gross = [x['Gross Charge'] for x in charges[0]]
            cash = [x['Discounted Cash Price'] for x in charges[0]]
            charges = pd.DataFrame(list(zip(code_type,code,gross,cash)))    
            charges.columns = ['vocabulary_id','concept_code','gross','cash']
            charges["vocabulary_id"] = charges["vocabulary_id"].str.lower()
            charges = cleanup_charges(
                charges = charges,
                rename = False,
                cash = "cash",
                gross = "gross",
                cpt = "cpt"
            )
            charges.to_json("./data/" + str(row["hospital_npi"]) + ".jsonl",lines=True,orient="records") 

        if row["idn"] == "Memorial":
            charges = load_json(row["file_url"])
            charges = charges["standard_charge_information"]         
            code_type = [x['billing_code_information'][0]['type'] for x in charges]
            cpt = [x['billing_code_information'][0]['code'] for x in charges]
            gross = [x['gross_charge'] if 'gross_charge' in x.keys() else None for x in [x['standard_charges'][0] for x in charges]]
            cash = [x['discounted_cash'] if 'discounted_cash' in x.keys() else None for x in [x['standard_charges'][0] for x in charges]]
            charges = pd.DataFrame(list(zip(code_type,code,gross,cash)))    
            charges.columns = ['vocabulary_id','concept_code','gross','cash']
            charges["vocabulary_id"] = charges["vocabulary_id"].str.lower()
            charges = cleanup_charges(
                charges = charges,
                rename = False,
                cash = "cash",
                gross = "gross",
                cpt = "cpt"
            )
            charges.to_json("./data/" + str(row["hospital_npi"]) + ".jsonl",lines=True,orient="records") 

        if row["idn"] == "Tennova Healthcare":
            if row["type"] == "CSV":
                with urllib.request.urlopen(row["file_url"]) as f:
                    charges = pd.read_csv(f,skiprows=int(row["skiprow"]),dtype="object",keep_default_na=False) 
                charges = cleanup_charges(
                    charges = charges,
                    rename = True,
                    gross = row["gross"],
                    cash = row["cash"],
                    cpt = row["cpt"]
                )
                charges.to_json("./data/" + str(row["hospital_npi"]) + ".jsonl",lines=True,orient="records") 

        if row["idn"] == 'Covenant Health':
            if row["type"] == "JSON":
                os.system('curl ' + row["file_url"] + " | jq > tmp.json")
                with open("tmp.json","r") as f:
                    charges = json.load(f)
                if os.path.exists("tmp.json"):
                    os.remove("tmp.json")                
                charges = charges['data']
                del charges[0]
                code_type = [x[0]['code type'] for x in charges]
                code = [x[0]['code'] for x in charges]
                gross = [x[0]['gross charge'] for x in charges]
                cash = [x[0]['discounted cash price'] for x in charges]
                charges = pd.DataFrame(list(zip(code_type,code,gross,cash)))    
                charges.columns = ['vocabulary_id','concept_code','gross','cash']
                charges = cleanup_charges(
                    charges = charges,
                    rename = False,
                    gross = "gross",
                    cash = "cash",
                    cpt = "cpt"
                )
                charges.to_json("./data/" + str(row["hospital_npi"]) + ".jsonl",lines=True,orient="records") 

        if os.path.exists("./data/" + str(row["hospital_npi"]) + ".jsonl"):
            status.append({
                "date": str(date.today()),
                "hospital_npi": row["hospital_npi"],
                "status": "SUCCESS",
                "file_url": row["file_url"]            
            })                
        else:
            status.append({
                "date": str(date.today()),
                "hospital_npi": row["hospital_npi"],
                "status": "WIP",
                "file_url": row["file_url"]            
            })
    except:
        status.append({
           "date": str(date.today()),
           "hospital_npi": row["hospital_npi"],
           "status": "FAILURE",
           "file_url": row["file_url"]            
       })


pd.DataFrame(status).sort_values(['hospital_npi']).to_csv("status.csv",index=False)
