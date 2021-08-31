TRUNCATE price;

COPY price FROM PROGRAM 'cat /opt/data/transformed/*.csv' WITH CSV;

COPY price TO PROGRAM 'gzip > /opt/data/extracts/price.csv.gz' WITH CSV HEADER;
