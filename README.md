[![YourActionName Actions Status](https://github.com/nathansutton/healthcare-price-transparency/workflows/CI/badge.svg)](https://github.com/nathansutton/healthcare-price-transparency/actions)

## Health Price transparency

Starting in NC

### Usage

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
