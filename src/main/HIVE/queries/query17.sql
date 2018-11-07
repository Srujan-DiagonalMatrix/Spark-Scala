DROP VIEW IF EXISTS ${MBNA_CUS_MIG_HIVE_DB_NAME};

CREATE VIEW IF NOT EXISTS ${MBNA_CUS_MIG_HIVE_DB_NAME}.email_view as
SELECT
    email.*
  FROM
    (
    (SELECT
      e.*
    FROM
      ${MBNA_CUS_MIG_HIVE_DB_NAME}.email e
    INNER JOIN
      ${MBNA_CUS_MIG_HIVE_DB_NAME}.gpdr_delta_rec
    ON
      e.pec_prod_acct_no = rec.prod_acct_no
    WHERE
      e.pec_email_priority_no = 1
      AND (e.pec_cntct_pnt_exp_dt > current_date() OR e.pec_cntct_pnt_exp_dt = '9999-12-31')
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
      AND paa_role_tp_no = '0001'
      AND email.pec_paa_seq_no = prd_paa_seq_no
    )
);