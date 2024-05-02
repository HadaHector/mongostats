import main as stat
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")

stat.initialize_connection(client,"statdb")

stats = {}

TestStat = stat.EventStat('test')
stats['testevent'] = TestStat

UsersStat = stat.StateStat('users',start_event="login",end_event="logout",magnitude_event="CCU",duration_event="sessionLength")
stats['users'] = UsersStat
stats['login'] = UsersStat.start_event
stats['logout'] = UsersStat.end_event
stats['ccu'] = UsersStat.magnitude_event
