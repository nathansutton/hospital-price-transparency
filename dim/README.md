# Dimension Tables

Reference data used by the scraper for validation and hospital metadata.

## Files

### CONCEPT.csv.gz

**Source:** [OHDSI Athena](https://athena.ohdsi.org/)

OMOP Common Data Model concept vocabulary containing CPT4 and HCPCS codes. Used to validate that scraped procedure codes are legitimate billing codes.

Download from Athena requires a free account. Select CPT4 and HCPCS vocabularies.

### urls/

**Source:** [hospitalpricingfiles.org](https://hospitalpricingfiles.org)

Per-state JSON files containing hospital price transparency file URLs. Each file maps CCN to the hospital's machine-readable price file URL.

Format:
```json
[
  {
    "ccn": "470011",
    "hospital_name": "Example Hospital",
    "file_url": "https://example.com/standardcharges.csv",
    "transparency_page": "https://example.com/pricing"
  }
]
```
