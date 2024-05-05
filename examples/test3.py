import mongostats as stat
from datetime import datetime
from pymongo import MongoClient
client = MongoClient("mongodb://localhost:27017/")
stat.initialize_connection(client,"statdb")

numstat = stat.NumericStat('num1')
numstat.on_event(10)
numstat.on_event(20)
numstat.on_event(-100)

#print(numstat.get_data_view(stat.EventInterval.MINUTE,datetime(2024,5,5,21,18),datetime(2024,5,5,21,28)))


multinumstat = stat.MultiNumericStat('num2')
multinumstat.on_event("A",1)
multinumstat.on_event("B",2)
multinumstat.on_event("C",10)
print(multinumstat.get_data_view(stat.EventInterval.MINUTE,datetime(2024,5,5,21,26),datetime(2024,5,5,21,30)))
