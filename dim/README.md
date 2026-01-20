# Dimension Tables

Reference data used by the scraper for validation and hospital metadata.

## Files

### CONCEPT.csv.gz

**Source:** [OHDSI Athena](https://athena.ohdsi.org/)

OMOP Common Data Model concept vocabulary containing CPT4 and HCPCS codes. Used to validate that scraped procedure codes are legitimate billing codes.

Download from Athena requires a free account. Select CPT4 and HCPCS vocabularies.

### place_of_service.csv

**Source:** [CMS Provider of Services File](https://data.cms.gov/provider-characteristics/hospitals-and-other-facilities/provider-of-services-file-quality-improvement-and-evaluation-system)

Hospital metadata including CCN (CMS Certification Number), name, address, and facility type. The Provider of Services file is updated quarterly by CMS.

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
