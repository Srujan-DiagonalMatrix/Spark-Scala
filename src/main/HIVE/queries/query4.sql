select
    o_orderpriority,
    count(*) as order_count
from
    orders as o
where
    o_orderdate >= '1996-05-01'
    and o_orderdate < '1996-08-01'
    and exists (
        select * from lineitem
        where
            l_orderkey = o.orderkey
            and l_commitdate < l_receiptdate
    )
group by o_orderpriority
sort by o_orderpriority;