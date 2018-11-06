-- This module is to insert eccr_egl hive table

USE ${MBNA_CUS_MIG_HIVE_DB_NAME};

INSERT INTO TABLE ${MBNA_CUS_MIG_HIVE_DB_NAME} PARTITION(month)
SELECT
  trim(urn),
  trim(record_id),
  trim(ref_type_no),
  trim(channel_cd),
  trim(account_no),
  trim(prod_account_no),
  trim(user_id),
  cast(regex_replace(concat(regex_replace(trm(created_ts), "_",""),"0"), '(\\d{4})(\\d{2})(\\d{2})(\\d{2})(\\d{2})(\\d{2})(\\d{1})','$1-$2-$3 $4:$5:$6.$7') as timestamp),
  ${batch_month}
FROM
  ${MBNA_CUS_MIG_HIVE_DB_NAME}.eccr_egl_stg
WHERE
  record_type="D";