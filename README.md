## Hospital Price transparency

The Centers for Medicare and Medicaid Services recently required hospitals under  [45 CFR §180.50](https://www.federalregister.gov/d/2019-24931/p-1010) to publish a [list of prices](https://www.cms.gov/hospital-price-transparency) on their websites.  They specifically instruct hospitals to make these lists...
- As a comprehensive machine-readable file with all items and services.   
- In a display of shoppable services in a consumer-friendly format.  

There is a lot of variation in adherence to these policies.  Without strong guidance on formatting from CMS, it is no wonder hospitals are all over the map on formatting.  Many hospitals have complied with the new rules but in ways that are not consumer friendly.  500 Megabytes of JSON data is not a strong start!

[Turquoise Health](https://turquoise.health/) has created a consumer-friendly lookup tool to interactively look up reported prices in different hospital systems. __This repository fills the gap with open data for researchers and data people.__

### Supplied Data

Each hospital is identified by the NPI.

A jsonlines file is create and tracked over time with version control according to the excellent pattern set out by [@simonw](https://github.com/simonw/ca-fires-history/tree/main).

```
{"cpt":"0031A","cash":12.8,"gross":56.53}
```
- __cpt__: the code from the AMA that corresponds to this billed service
- __gross__: this is often the top line item that the hospital never actually charges  
- __cash__: this is the self-pay discounted price you would pay without insurance


### Ontology

We rely on the excellent work of the [Athena](https://athena.ohdsi.org/) vocabulary to define the ontology of healthcare procedures.  This maps [CPT](https://www.ama-assn.org/practice-management/cpt) and [HCPCS](https://www.cms.gov/Medicare/Coding/MedHCPCSGenInfo) codes into a [common data model](https://github.com/OHDSI/CommonDataModel).

The disadvantage with this normalization is that we exclude the hospital-specific items such as their room and board charges. For example, 'deluxe single' does not confer the same charge in different hospital systems.


### Coverage

I am starting in CBSA's in Southern Appalachia.
- Asheville, NC
- Johnson City, TN
- Knoxville, TN
- Chattanooga, TN

### A word of caution

I took great care to make the data generation process reproducible inside of a docker container. However, I sacrificed some scalability for the name of speed of development. There are some [excellent examples]((https://github.com/vsoch/hospital-chargemaster/blob/master/hospitals.tsv)) how you could scrape your way through this to complete automation. I introduced a manual step of downloading a file and naming it by my generated hospital ID. All other transformations are codified and reproducible in the container.

## Where to go from here
I will use these data as a launching point to investigate questions around the economics of hospital prices. The New York Times had a great quote in their recent article ['Hospitals and Insurers Didn't Want You to See These Prices. Here's Why'](https://www.nytimes.com/interactive/2021/08/22/upshot/hospital-prices.html?smid=url-share).

```
The trade association for insurers said it was "an anomaly" that some insured patients got worse prices than those paying cash.
```

__This seems like an excellent question for data people to answer.__


### Contact

Submit an issue if you find anything inconsistent.  Like all data products, we make assumptions and provide no warrantee.  
