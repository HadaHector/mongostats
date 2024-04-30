import main as stat

stat.initialize("mongodb://localhost:27017/","statdb")
TestStat = stat.EventState('users',startEvent="login",endEvent="logout",magnitudeEvent="CCU",durationEvent="sessionLength")

TestStat.onStartEvent("sanyi")
TestStat.onEndEvent("bela")
TestStat.onInterval()