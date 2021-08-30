CREATE TABLE price_pivot (
  hospital_id           BIGINT NOT NULL,
  concept_id            INTEGER NOT NULL,
  gross_amount          FLOAT NULL,
  cash_amount           FLOAT NULL,
  max_amount            FLOAT NULL,
  min_amount            FLOAT NULL
)
;
