TRUNCATE price;

COPY price FROM PROGRAM 'cat /opt/data/transformed/*.csv' WITH CSV;
