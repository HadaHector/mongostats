import main as stat

stat.initialize("mongodb://localhost:27017/","statdb")

stats = {}

TestStat = stat.EventStat('test')
stats['testevent'] = TestStat

UsersStat = stat.EventState('users',startEvent="login",endEvent="logout",magnitudeEvent="CCU",durationEvent="sessionLength")
stats['login'] = UsersStat.startEvent
stats['logout'] = UsersStat.endEvent
stats['ccu'] = UsersStat.magnitudeEvent
