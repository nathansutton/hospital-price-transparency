[![YourActionName Actions Status](https://github.com/nathansutton/healthcare-price-transparency/workflows/CI/badge.svg)](https://github.com/nathansutton/healthcare-price-transparency/actions)

## Hospital Price transparency

The Centers for Medicare and Medicaid Services recently required hospitals under  [45 CFR §180.50](https://www.federalregister.gov/d/2019-24931/p-1010) to publish a [list of prices](https://www.cms.gov/hospital-price-transparency) on their websites.  They specifically instruct hospitals to make these lists...
- As a comprehensive machine-readable file with all items and services.   
- In a display of shoppable services in a consumer-friendly format.  

There is a lot of variation in adherence to these policies.  Without strong guidance on formatting from CMS, it is no wonder hospitals are all over the map on formatting.  Many hospitals have complied with the new rules but in ways that are not consumer friendly.  500 Megabytes of JSON data is not a strong start!

__This repository cuts out pricing noise purposefully introduced by these hospital systems__.  You can easily search for a given CPT or HCPCS code and compare those prices across hospitals.  

### Supplied Data

If you don't have the proclivity to transform these data yourself with docker, there are CSV extracts available in ./volumes/data/extracts.  They are broken down into four distinct groups.

- __gross__: this is often the top line item that the hospital never actually charges  
- __cash__: this is the self-pay discounted price you would pay without insurance
- __max__: this is the maximum negotiated rate by an insurance company in the hospital network.
- __min__: this is the minimum negotiated rate by an insurance company in the hospital network

### Ontology

We rely on the excellent work of the [Athena](https://athena.ohdsi.org/) vocabulary to define the ontology of healthcare procedures.  This maps [CPT](https://www.ama-assn.org/practice-management/cpt) and [HCPCS](https://www.cms.gov/Medicare/Coding/MedHCPCSGenInfo) codes into a [common data model](https://github.com/OHDSI/CommonDataModel).

### Coverage

Only North Carolina is covered right now because I happen to live there.  Submit a PR if you have found data for other hospital systems.  

### Usage

Quickstart with docker-compose
```
docker-compose up
```

Run the flyway migrations
```
docker-compose run flyway
```

Run the ETL
```
docker-compose run etl
```

Interactive PSQL client
```
docker exec -it postgres psql -d postgres -U builder
```

### Contact

Submit an issue if you find anything inconsistent.  Like all data products, we make assumptions and provide no warrantee.  
