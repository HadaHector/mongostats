from datetime import datetime,timedelta
from enum import Enum
from pymongo import MongoClient, errors
from enum import IntEnum

dbclient = None
database = None

def initialize(mongourl,dbname):
	global dbclient, database
	dbclient = MongoClient(mongourl)
	database = dbclient[dbname]

class ConfigException(Exception):
	pass

class EventInterval(IntEnum):
	SECOND = 1
	MINUTE = 2
	HOUR = 3
	DAY = 4
	MONTH = 5

	def __str__(self):
		return self.name

class StatBase:
	def __init__(self,name:str,minInterval:EventInterval=EventInterval.MINUTE,maxInterval:EventInterval=EventInterval.MONTH) -> None:
		super()
		self.name = name
		self.intervals = []
		for i in range(minInterval.value,maxInterval.value+1):
			self.intervals.append(EventInterval(i))
		
		if not len(self.intervals):
			raise ConfigException("There are no intervals in this stat")
	
	def getDatetimeForInterval(interval:EventInterval,time=None) -> datetime:
		if not time:
			now = datetime.now(tz=None)
		else:
			now = time
		if interval.value >= EventInterval.MONTH.value:
			now = now.replace(day = 1)
		if interval.value >= EventInterval.DAY.value:
			now = now.replace(hour = 0)
		if interval.value >= EventInterval.HOUR.value:
			now = now.replace(minute = 0)
		if interval.value >= EventInterval.MINUTE.value:
			now = now.replace(second = 0)
		if interval.value >= EventInterval.SECOND.value:
			now = now.replace(microsecond = 0)
		return now

	def getPrevDatetimeForInterval(interval:EventInterval,time=None) -> datetime:
		if not time:
			now = datetime.now(tz=None)
		else:
			now = time
		now = StatBase.getDatetimeForInterval(interval,now)
		
		if interval == EventInterval.MONTH:
			if now.month == 1:
				now = now.replace(year=now.year-1,month=12)
			else:
				now = now.replace(month=now.month-1)
		elif interval == EventInterval.DAY:
			now = now + timedelta(days=-1)
		elif interval == EventInterval.HOUR:
			now = now + timedelta(hours=-1)
		elif interval == EventInterval.MINUTE:
			now = now + timedelta(minutes=-1)
		elif interval == EventInterval.SECOND:
			now = now + timedelta(seconds=-1)
		return now
	


class EventStat(StatBase):
	
	def onEvent(self) -> None:
		global database
		
		smallestInterval = self.intervals[0]
		time = StatBase.getDatetimeForInterval(smallestInterval)

		coll = database[self.name+"_"+str(smallestInterval)]
		coll.update_one({"_id":time},{"$inc":{"value":1}},True)
	
	def onInterval(self) -> None:
		global database
		#let's assume this is called every second smallest interval
		if len(self.intervals) < 2:
			return
		
		now = datetime.now(tz=None)

		for i in range(1,len(self.intervals)):
			interval = self.intervals[i]
			
			#check if the result exists and create the aggregation if needed
			time = StatBase.getPrevDatetimeForInterval(interval,now) #start of the measure window
			currenttime = StatBase.getDatetimeForInterval(interval,now) #end of the measure window

			#collection to collect the data from
			coll = database[self.name+"_"+str(EventInterval(interval.value-1))]
			colltarget = database[self.name+"_"+str(interval)]
	
			if colltarget.count_documents({"_id":time}) == 0:
				coll.aggregate([
					{"$match":{"_id":{"$lt":currenttime,"$gte":time}}},
					{"$group":{"_id":time,"value":{"$sum":"$value"}}},
					{"$out":{"db": database.name,"coll":self.name+"_"+str(interval)}}
				])

				#if the $match is empty, no document is created
				if colltarget.count_documents({"_id":time}) == 0:
					colltarget.insert_one({"_id":time,"value":0})
				

class EventState(StatBase):

	#todo add TTL support
	def __init__(self, name: str, startEvent:str=None, endEvent:str=None, magnitudeEvent:str=None, durationEvent:str=None, minInterval: EventInterval = EventInterval.MINUTE, maxInterval: EventInterval = EventInterval.MONTH, ttl=None) -> None:
		super().__init__(name, minInterval, maxInterval)

		self.startEvent = EventStat(startEvent)
		self.startEvent.intervals = self.intervals
		self.endEvent = EventStat(endEvent)
		self.endEvent.intervals = self.intervals
		self.magnitudeEvent = magnitudeEvent
		self.durationEvent = durationEvent

	def onStartEvent(self,id):
		global database

		coll = database[self.name+"_SESSION"]
		try:
			coll.insert_one({"_id":id,"created":datetime.now()})
		except errors.DuplicateKeyError:
			self.onEndEvent(id)
			self.onStartEvent(id)
			return

		if self.startEvent:
			self.startEvent.onEvent()
	
	def onEndEvent(self,id):
		global database

		coll = database[self.name+"_SESSION"]
		doc = coll.find_one_and_delete({"_id":id})
		if  not doc:
			return

		if self.endEvent:
			self.endEvent.onEvent()

		if self.durationEvent:
			delta = datetime.now() - doc["created"]
			duration = delta.total_seconds()

			coll = database[self.durationEvent]
			coll.insert_one({"duration":duration,"endTime":datetime.now()})
	
	def onInterval(self) -> None:

		now = datetime.now(tz=None)

		if self.startEvent:
			self.startEvent.onInterval()
		if self.endEvent:
			self.endEvent.onInterval()
		
		#magnitude
		smallestInterval = self.intervals[0]
		time = StatBase.getDatetimeForInterval(smallestInterval,now) #end of the measure window

		if self.magnitudeEvent:
			sessioncoll = database[self.name+"_SESSION"]
			count = sessioncoll.count_documents({})

			coll = database[self.magnitudeEvent+"_"+str(smallestInterval)]
			coll.insert_one({"_id":time,"value":count},True)

		for i in range(1,len(self.intervals)):
			interval = self.intervals[i]
			
			#check if the result exists and create the aggregation if needed
			time = StatBase.getPrevDatetimeForInterval(interval,now) #start of the measure window
			currenttime = StatBase.getDatetimeForInterval(interval,now) #end of the measure window

			#collection to collect the data from
			coll = database[self.magnitudeEvent+"_"+str(EventInterval(interval.value-1))]
			colltarget = database[self.magnitudeEvent+"_"+str(interval)]
	
			if colltarget.count_documents({"_id":time}) == 0:
				coll.aggregate([
					{"$match":{"_id":{"$lt":currenttime,"$gte":time}}},
					{"$group":{"_id":time,"value":{"$max":"$value"}}},
					{"$out":{"db": database.name,"coll":self.magnitudeEvent+"_"+str(interval)}}
				])

				#if the $match is empty, no document is created
				if colltarget.count_documents({"_id":time}) == 0:
					colltarget.insert_one({"_id":time,"value":0})
		









