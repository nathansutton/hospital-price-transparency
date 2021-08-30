TRUNCATE price_pivot;

INSERT INTO price_pivot
SELECT
    hospital_id
  , concept_id
  , MAX(CASE WHEN price = 'gross' THEN amount ELSE NULL END) AS gross_amount
  , MAX(CASE WHEN price = 'cash' THEN amount ELSE NULL END) AS cash_amount
  , MAX(CASE WHEN price = 'max' THEN amount ELSE NULL END) AS max_amount
  , MAX(CASE WHEN price = 'min' THEN amount ELSE NULL END) AS min_amount
FROM price
JOIN concept
  USING (concept_id)
GROUP BY
    hospital_id
  , concept_id
;
