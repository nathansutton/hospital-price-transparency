TRUNCATE hospital;

COPY hospital FROM '/opt/data/dim/hospital.csv' WITH CSV HEADER;
