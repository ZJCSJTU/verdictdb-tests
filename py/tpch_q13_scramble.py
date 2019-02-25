import pyverdict
import time
import sys

filename = sys.argv[1]

verdict = pyverdict.presto('localhost', 'hive', 'jiangchen', port=9080)
# verdict.sql('use tpch10g')
query = """select
        c_count,
        count(*) as custdist
from
        (
        select
                c_custkey,
                count(o_orderkey) as c_count
        from
                tpch10g.customer left outer join tpch10g.orders_scramble on
                c_custkey = o_custkey
        where o_comment not like '%special%requests%'
        group by
                c_custkey
        ) as c_orders
group by
        c_count
order by
        custdist desc,
        c_count desc;"""

start_time = time.time()
verdict.sql(query)
end_time = time.time()

f = open(filename, 'a')
f.write(str(end_time - start_time) + " ")
