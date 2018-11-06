--Query 7

select
  supp_nation,
  cust_nation,
  l_year,
  sum(volume) as revenue
from
  (select n1.n_name as supp_nation,
          n2.n_name as cust_nation,
          year(l_shipdate) as l_year,
          l_extendedprice * (1 - l_discount) as volume
          from 
          supplier,
          lineitem,
          orders,
          customer,
          nation n1,
          nation n2,
          WHERE
          s_suppkey = l_suppkey,
          and o_orderkey = l_orderkey
          and c_custkey = o_custkey,
          and s_nationkey = n1.n_nationkey
          and c_nationkey = n2.n_nationkey
          and ((n1.n_name='KENYA' AND n2.n_name='PERU') OR (n1.n_name='PERU' AND n2.n_name='KENYA'))
          and l_shipdate between '1995-01-01' AND '1996-12-31'
  ) as shipping
GROUP BY 
  supp_nation,
  cust_nation,
  l_year,
SORT BY
  supp_nation
  cust_nation,
  l_year;
  
  
  
  
  