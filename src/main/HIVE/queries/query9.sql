--Query 9

SELECT
  nation,
  o_year,
  sum(amount) as sum_profit
FROM
  (
  SELECT
    n_name as nation,
    year(o_orderdate) as o_year,
    l_extendedprice * (1-l_discount) - ps_supplycost * l_quantity as amount
  FROM
    part,
    supplier,
    lineitem,
    partsupp,
    orders,
    nation
  WHERE
    s_suppkey = l_suppkey
    AND ps_suppkey = l_suppkey
    AND ps_partkey = l_partkey
    AND p_partkey = l_partkey
    AND o_orderkey = l_orderKey
    AND s_nationkey = n_nationkey
    AND p_name LIKE '%plum%'
  ) as profit
  GROUP BY 
    nation,
    o_year
  SORT BY
    nation,
    o_year;