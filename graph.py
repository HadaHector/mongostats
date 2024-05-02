import plotly.express as px
import main as stat
from datetime import datetime
import pandas as pd

stat.initialize("mongodb://localhost:27017/","statdb")
TestStat = stat.EventStat('test')
keys,values = TestStat.getDataView(stat.EventInterval.MINUTE,startDate=datetime(2024, 4, 30, 13, 30),endDate=datetime(2024, 4, 30, 15, 30))

df = pd.DataFrame(dict(values=values))

fig = px.line(x=keys,y="values",data_frame=df,title='test',labels=dict(values="count"))
fig.write_html("output/test.html")