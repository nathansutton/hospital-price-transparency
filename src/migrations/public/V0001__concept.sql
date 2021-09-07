CREATE TABLE concept
(
  concept_id         INTEGER NOT NULL,
  concept_name       VARCHAR(255) NULL,
  domain_id          VARCHAR(20) NOT NULL,
  vocabulary_id      VARCHAR(20) NOT NULL,
  concept_class_id   VARCHAR(20) NOT NULL,
  standard_concept   VARCHAR(1) NULL,
  concept_code       VARCHAR(50) NOT NULL,
  valid_start_date   DATE NOT NULL,
  valid_end_date     DATE NOT NULL,
  invalid_reason     VARCHAR(1) NULL
);

COPY concept FROM PROGRAM 'gunzip -c /opt/data/dim/CONCEPT.csv.gz' WITH DELIMITER E'\t' CSV HEADER QUOTE E'\b' ;
