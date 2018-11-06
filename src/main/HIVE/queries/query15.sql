DROP VIEW IF EXISTS ${MBNA_CUS_MIG_HIVE_DB_NAME}.email_view;

CREATE VIEW IF NOT EXISTS ${MBNA_CUS_MIG_HIVE_DB_NAME}.email_view as
SELECT *
FROM
  (
    (SELECT e.*
    FROM
      ${MBNA_CUS_MIG_HIVE_DB_NAME}.email e
    INNER JOIN
      ${MBNA_CUS_MIG_HIVE_DB_NAME}.gdpr_delta_rec rec
    ON
      e.pec_prod_acct = rec.prod_acct
    WHERE
      e.pec_email_priority_no = 1
      AND (e.pec_cntct_pnt_exp_dt > currentdate() OR e.pec_cntct_pnt_exp_dt = '9999-12-31')
      AND e.month=${batch_month}
      AND rec.month=${batch_month}
      AND rec.batch_id=${batch_id}
  ) email
  INNER JOIN
  (
    SELECT
      prod_acct_no,
      paa_role_tp_no,
      paa_seq_no
    FROM
      ${MBNA_CUS_MIG_HIVE_DB_NAME}.prdaccta
    WHERE
      month=${batch_month}
  ) prd
  ON
  (email.pec_prod_acct_no = prd.prd_acct_no
   AND prd.paa_role_tp_no = prd.paa_seq_no)
);