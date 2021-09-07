[![YourActionName Actions Status](https://github.com/nathansutton/healthcare-price-transparency/workflows/CI/badge.svg)](https://github.com/nathansutton/healthcare-price-transparency/actions)

## Hospital Price transparency

The Centers for Medicare and Medicaid Services recently required hospitals under  [45 CFR §180.50](https://www.federalregister.gov/d/2019-24931/p-1010) to publish a [list of prices](https://www.cms.gov/hospital-price-transparency) on their websites.  They specifically instruct hospitals to make these lists...
- As a comprehensive machine-readable file with all items and services.   
- In a display of shoppable services in a consumer-friendly format.  

There is a lot of variation in adherence to these policies.  Without strong guidance on formatting from CMS, it is no wonder hospitals are all over the map on formatting.  Many hospitals have complied with the new rules but in ways that are not consumer friendly.  500 Megabytes of JSON data is not a strong start!

[Turquoise Health](https://medium.com/r/?url=https%3A%2F%2Fturquoise.health%2F) has created a consumer-friendly lookup tool to interactively look up reported prices in different hospital systems. However, my guess is that they would not be happy to sharing the underlying data they have monetized (but I will ask). This repository fills the gap with open data for researchers and data people.
### Supplied Data

If you don't have the proclivity to transform these data yourself with docker, there are CSV extracts available in ./volumes/data/extracts.  They are broken down into four distinct groups.

- __gross__: this is often the top line item that the hospital never actually charges  
- __cash__: this is the self-pay discounted price you would pay without insurance
- __max__: this is the maximum negotiated rate by an insurance company in the hospital network.
- __min__: this is the minimum negotiated rate by an insurance company in the hospital network

A minority of hospitals included the payer and plan specific charges as their own column. I found that hospitals much more frequently reported the de-identified maximum and minimum negotiated prices, and so I started there.

### Ontology

We rely on the excellent work of the [Athena](https://athena.ohdsi.org/) vocabulary to define the ontology of healthcare procedures.  This maps [CPT](https://www.ama-assn.org/practice-management/cpt) and [HCPCS](https://www.cms.gov/Medicare/Coding/MedHCPCSGenInfo) codes into a [common data model](https://github.com/OHDSI/CommonDataModel).

The disadvantage with this normalization is that we exclude the hospital-specific items such as their room and board charges. For example, 'deluxe single' does not confer the same charge in different hospital systems.


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

### A word of caution

I took great care to make the data generation process reproducible inside of a docker container. However, I sacrificed some scalability for the name of speed of development. There are some [excellent examples]((https://github.com/vsoch/hospital-chargemaster/blob/master/hospitals.tsv)) how you could scrape your way through this to complete automation. I introduced a manual step of downloading a file and naming it by my generated hospital ID. All other transformations are codified and reproducible in the container.

## Where to go from here
I will use these data as a launching point to investigate questions around the economics of hospital prices. The New York Times had a great quote in their recent article ['Hospitals and Insurers Didn't Want You to See These Prices. Here's Why'](https://medium.com/r/?url=https%3A%2F%2Fwww.nytimes.com%2Finteractive%2F2021%2F08%2F22%2Fupshot%2Fhospital-prices.html%3Fsmid%3Durl-share).

```
The trade association for insurers said it was "an anomaly" that some insured patients got worse prices than those paying cash.
```

__This seems like an excellent question for data people to answer.__


### Contact

Submit an issue if you find anything inconsistent.  Like all data products, we make assumptions and provide no warrantee.  
