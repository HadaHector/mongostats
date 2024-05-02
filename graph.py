import plotly.express as px
import main as stat
from datetime import datetime
import pandas as pd

stat.initialize("mongodb://localhost:27017/","statdb")
TestStat = stat.EventStat('test')
keys,values = TestStat.get_data_view(stat.EventInterval.MINUTE,start_date=datetime(2024, 4, 30, 13, 30),end_date=datetime(2024, 4, 30, 15, 30))

df = pd.DataFrame(dict(values=values))

fig = px.line(x=keys,y="values",data_frame=df,title='test',labels=dict(values="count"))
fig.write_html("output/test.html")