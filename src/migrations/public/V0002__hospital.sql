CREATE TABLE hospital
(
  hospital_id           BIGINT NOT NULL,
  hospital_name         VARCHAR(500),
  hospital_npi          VARCHAR(15),
  city                  VARCHAR(50) NULL,
  state                 VARCHAR(2) NULL,
  critical_access_ind   INTEGER,
  affiliation           VARCHAR(100) NULL,
  pricing_url           VARCHAR(500) NULL
);

ALTER TABLE hospital ADD CONSTRAINT xpk_hospital PRIMARY KEY (hospital_id);

CREATE UNIQUE INDEX idx_hospital_hospital_id ON hospital (hospital_id ASC);
