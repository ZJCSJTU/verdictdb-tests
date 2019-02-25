import pyverdict
import time
import sys

filename = sys.argv[1]

verdict = pyverdict.presto('localhost', 'hive', 'jiangchen', port=9080)
# verdict.sql('use tpch10g')
query = """select
        o_orderpriority,
        count(*) as order_count
from
        tpch10g.orders_scramble join tpch10g.lineitem_scramble on l_orderkey = o_orderkey
where
        o_orderdate >= date '1993-07-01'
        and o_orderdate < date '1998-12-01'
        and l_commitdate < l_receiptdate
group by
        o_orderpriority
order by
        o_orderpriority;"""


start_time = time.time()
df = verdict.sql(query)
end_time = time.time()

f = open(filename, 'a')
f.write(str(end_time - start_time))
