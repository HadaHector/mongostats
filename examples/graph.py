import plotly.express as px
import mongostats as stat
from datetime import datetime
import pandas as pd

from pymongo import MongoClient
client = MongoClient("mongodb://localhost:27017/")
stat.initialize_connection(client,"statdb")

TestStat = stat.EventStat('test')
keys,values = TestStat.get_data_view(stat.EventInterval.MINUTE,start_date=datetime(2024, 4, 30, 13, 30),end_date=datetime(2024, 4, 30, 15, 30))

df = pd.DataFrame(dict(values=values))

fig = px.line(x=keys,y="values",data_frame=df,title='test',labels=dict(values="count"))
fig.write_html("output/test.html")