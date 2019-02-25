import pyverdict
import time
import sys

filename = sys.argv[1]
print(filename)
verdict = pyverdict.presto('localhost', 'hive', 'jiangchen', port=9080)
verdict.sql('use tpch10g')
query = """select
        l_returnflag,
        l_linestatus,
        sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc,
        count(*) as count_order
from
        tpch10g.lineitem_scramble
where
        l_shipdate <= date '1998-12-01'
group by
        l_returnflag,
        l_linestatus
order by
        l_returnflag,
        l_linestatus;"""

start_time = time.time()
df = verdict.sql(query)
end_time = time.time()

f = open(filename, 'a')
f.write(str(end_time - start_time) + " ")
