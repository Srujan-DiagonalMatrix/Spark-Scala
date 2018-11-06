-- This module is to insert eccr_grt hive table


USE ${MBNA_CUS_MIG_HIVE_DB_NAME};

INSERT INTO TABLE ${MBNA_CUS_MIG_HIVE_DB_NAME}.eccr_grt PARTITION(month)
SELECT
  ref_type_no,
  type_cd,
  ref_type_cd,
  desc_tx,
  user_id,
  created_dt,
  updated_dt
  ${batch_month}
FROM
  ${MBNA_CUS_MIG_HIVE_DB_NAME}.eccer_grt_stg
WHERE
  record_type="D";