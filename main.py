from datetime import datetime,timedelta
from enum import Enum
import pymongo
from enum import IntEnum

import pymongo.collection

dbclient = None
database = None

def initialize(mongourl,dbname):
	global dbclient, database
	dbclient = pymongo.MongoClient(mongourl)
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
	
	def getPrevInterval(interval:EventInterval,time) -> datetime:
		if interval == EventInterval.MONTH:
			if time.month == 1:
				time = time.replace(year=time.year-1,month=12)
			else:
				time = time.replace(month=time.month-1)
		elif interval == EventInterval.DAY:
			time = time + timedelta(days=-1)
		elif interval == EventInterval.HOUR:
			time = time + timedelta(hours=-1)
		elif interval == EventInterval.MINUTE:
			time = time + timedelta(minutes=-1)
		elif interval == EventInterval.SECOND:
			time = time + timedelta(seconds=-1)
		return time
	
	def getNextInterval(interval:EventInterval,time) -> datetime:
		if interval == EventInterval.MONTH:
			if time.month == 12:
				time = time.replace(year=time.year+1,month=1)
			else:
				time = time.replace(month=time.month+1)
		elif interval == EventInterval.DAY:
			time = time + timedelta(days=1)
		elif interval == EventInterval.HOUR:
			time = time + timedelta(hours=1)
		elif interval == EventInterval.MINUTE:
			time = time + timedelta(minutes=1)
		elif interval == EventInterval.SECOND:
			time = time + timedelta(seconds=1)
		return time
	
	def getPrevDatetimeForInterval(interval:EventInterval,time=None) -> datetime:
		if not time:
			now = datetime.now(tz=None)
		else:
			now = time
		now = StatBase.getDatetimeForInterval(interval,now)
		now = StatBase.getPrevInterval(interval,now)
		return now
	
	def getNextDatetimeForInterval(interval:EventInterval,time=None) -> datetime:
		if not time:
			now = datetime.now(tz=None)
		else:
			now = time
		now = StatBase.getDatetimeForInterval(interval,now)
		now = StatBase.getNextInterval(interval,now)
		return now

class EventStat(StatBase):
	
	def getCollection(self,interval:EventInterval) -> pymongo.collection:
		global database
		return database[self.name+"_"+str(interval)]
	
	def onEvent(self) -> None:		
		smallestInterval = self.intervals[0]
		time = StatBase.getDatetimeForInterval(smallestInterval)

		coll = self.getCollection(smallestInterval)
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
			coll = self.getCollection(EventInterval(interval.value-1))
			colltarget = self.getCollection(interval)
	
			if colltarget.count_documents({"_id":time}) == 0:
				coll.aggregate([
					{"$match":{"_id":{"$lt":currenttime,"$gte":time}}},
					{"$group":{"_id":time,"value":{"$sum":"$value"}}},
					{"$out":{"db": database.name,"coll":self.name+"_"+str(interval)}}
				])

				#if the $match is empty, no document is created
				if colltarget.count_documents({"_id":time}) == 0:
					colltarget.insert_one({"_id":time,"value":0})
	
	def getDataView(self,interval:EventInterval,startDate:datetime,endDate:datetime):
		coll = self.getCollection(interval)
		documents = list(coll.find({"_id":{"$gte":startDate,"$lte":endDate}},sort=[('_id', pymongo.ASCENDING)]))

		keys = []
		values = []

		key = StatBase.getPrevDatetimeForInterval(interval,startDate)
		docidx = 0
		while True:
			keys.append(key)
			if docidx<len(documents) and documents[docidx]["_id"] == key:
				values.append(documents[docidx]["value"])
				docidx += 1
			else:
				values.append(0)
			key = StatBase.getNextInterval(interval,key)
			if key > endDate:
				break
		
		return keys,values


class EventState(StatBase):

	def __init__(self, name: str, startEvent:str=None, endEvent:str=None, magnitudeEvent:str=None, durationEvent:str=None, minInterval: EventInterval = EventInterval.MINUTE, maxInterval: EventInterval = EventInterval.MONTH, expireAfterSeconds=None) -> None:
		super().__init__(name, minInterval, maxInterval)

		self.startEvent = EventStat(startEvent)
		self.startEvent.intervals = self.intervals
		self.endEvent = EventStat(endEvent)
		self.endEvent.intervals = self.intervals
		self.magnitudeEvent = EventStat(magnitudeEvent)
		self.durationEvent = durationEvent

		if expireAfterSeconds:
			global database
			self.useTtl = True
			self.getSessionCollection().create_index({"expires":1},expireAfterSeconds=expireAfterSeconds)

	def getSessionCollection(self):
		return database[self.name+"_SESSION"]
	
	def onStartEvent(self,id):
		global database

		coll = self.getSessionCollection()
		try:
			coll.insert_one({"_id":id,"created":datetime.now()})
		except pymongo.errors.DuplicateKeyError:
			self.onEndEvent(id)
			self.onStartEvent(id)
			return

		if self.startEvent:
			self.startEvent.onEvent()
	
	def onEndEvent(self,id):
		global database

		coll = self.getSessionCollection()
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
			sessioncoll = self.getSessionCollection()
			count = sessioncoll.count_documents({})

			coll = self.magnitudeEvent.getCollection(smallestInterval)
			coll.insert_one({"_id":time,"value":count},True)

		for i in range(1,len(self.intervals)):
			interval = self.intervals[i]
			
			#check if the result exists and create the aggregation if needed
			time = StatBase.getPrevDatetimeForInterval(interval,now) #start of the measure window
			currenttime = StatBase.getDatetimeForInterval(interval,now) #end of the measure window

			#collection to collect the data from
			coll = self.magnitudeEvent.getCollection(EventInterval(interval.value-1))
			colltarget = self.magnitudeEvent.getCollection(interval)
	
			if colltarget.count_documents({"_id":time}) == 0:
				coll.aggregate([
					{"$match":{"_id":{"$lt":currenttime,"$gte":time}}},
					{"$group":{"_id":time,"value":{"$max":"$value"}}},
					{"$out":{"db": database.name,"coll":self.magnitudeEvent+"_"+str(interval)}}
				])

				#if the $match is empty, no document is created
				if colltarget.count_documents({"_id":time}) == 0:
					colltarget.insert_one({"_id":time,"value":0})
		









