-- This module is to insert eccr_eef hive table


USE ${MBNA_CUS_MIG_HIVE_DB_NAME};

INSERT INTO ${MBNA_MIG_HIVE_DB_NAME}.eccr_eef PARTITION (month)
SELECT
  trim(URN),
  trim(ref_type_no),
  trim(old_value_tx),
  trim(new_value_tx),
  trim(data_type_dx),
  ${batc_month}
FROM
  ${MBNA_MIG_HIVE_DB_NAME}.eccr_eef_stg
WHERE
  record_type = 'D';