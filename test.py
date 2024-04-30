import main as stat

stat.initialize("mongodb://localhost:27017/","statdb")
TestStat = stat.EventStat('test')
TestStat.onEvent()
TestStat.onInterval()