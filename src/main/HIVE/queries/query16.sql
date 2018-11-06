-- Identify Whitelist and process the same to staging.

add jar /usr/hdp/current/hive-client/lib/hive-contrib.jar;

INSERT OVERWRITE TABLE ${MBNA_CUS_MIG_HIVE_DB_NAME}.gdpr_delta_rec PARTITION (month, batch_id)
SELECT
    delta.prod_acct_no,
    urn,
    created_ts,
    ${batch_month},
    ${batch_id}
  FROM
    (
      SELECT 
        prod_acct_no,
        egl.urn,
        created_ts,
        ${batch_month},
        ${batch_id}
      FROM
        (SELECT DISTINCT
          cur.urn
        FROM  
          (
            SELECT 
              urn
            FROM
              ${MBNA_CUS_MIG_HIVE_DB_NAME}.eccr_eef
            WHERE
              month=${batch_month}
          ) cur
        LEFT OUTER JOIN
          (
            SELECT
              ur
            FROM
              ${MBNA_CUS_MIG_HIVE_DB_NAME}.eccr_eef
            WHERE
              month=${batch_month_prev}
          ) prev
          ON
            cur.urn = prev.urn
        )
      WHERE
        prev.urn IS NULL
    ) urn
    
    INNER JOIN
    
    (
      SELECT
        prod_acct_no,
        craeted_ts,
        urn
      FROM
        (
          SELECT
            prod_acct_no,
            created_ts,
            urn,
            Row_number() OVER 
              (
                PARTITIONED BY 
                  prd_acct_no
                SORT BY
                  created_ts DESC,
                  urn DESC
              ) as latest_row
            FROM
              ${MBNA_CUS_MIG_HIVE_DB_NAME}.eccr_egl
            WHERE
              egl_in.latest_row=1
        ) egl
        ON
          urn.urn = egl.urn
        ) delta
        LEFT OUTER JOIN
        (
          SELECT
            prod_acct_no
          FROM
            ${MBNA_CUS_MIG_HIVE_DB_NAME}.gdpr_bl
        ) bl
        ON
          delta.prod_acct_no = bl.prod_acct_no
        WHERE
          bl.prod_acct_no IS NULL;