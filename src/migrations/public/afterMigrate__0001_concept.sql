COPY (
  SELECT
      concept_id
    , concept_code
    , vocabulary_id
    , concept_name
  FROM concept
  WHERE
      vocabulary_id IN ('CPT','HCPCS')
  ORDER BY
      vocabulary_id
    , concept_name
) TO PROGRAM 'gzip > /opt/data/extracts/concept.csv.gz' WITH CSV HEADER;
