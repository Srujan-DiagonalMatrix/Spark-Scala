--Query 8

SELECT
  o_year,
  SUM(
    CASE 
      WHEN nation='PERU' THEN volume
      ELSE 0
    END
    )/SUM(volume) as mkt_share
FROM
  (
    SELECT 
    year(o_orderdate) as o_year,
    l_extendedprice * (1 - l_discount) as volume,
    n2.n_name as nation
    FROM
      part,
      supplier,
      lineitem,
      orders,
      customer,
      nation n1,
      nation n2,
      region
    WHERE
      p_partkey = l_partkey,
      AND s_suppkey = l-suppkey,
      AND l_orderkey = o_orderkey,
      AND o_custkey = c_cstkey,
      AND c_nationkey = n1.n.nationkey
      AND n1.n_regionkey = r_regionkey
      AND r_name = 'AMERICA'
      AND s_nationkey = n2.n_nationkey
      AND o_orderdate BETWEEN '1995-01-01' AND '1996-12-31'
      AND p_type = 'ECONAMY BURNISHED NICKEL'
  ) AS all_nations
GROUP BY o_year,
SORT BY o_year;