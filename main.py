from datetime import datetime,timedelta
import pymongo
from enum import IntEnum

import pymongo.collection

dbclient = None
database = None

def initialize(mongourl,dbname):
	"""
	This function initializes the database connection for the statistics
	"""
	global dbclient, database
	dbclient = pymongo.MongoClient(mongourl)
	database = dbclient[dbname]

class ConfigException(Exception):
	pass

class EventInterval(IntEnum):
	"""
	Statistics measuring interval
	"""
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
	"""
	Basic event based statistics.
	"""
	def __init__(self, name: str, minInterval: EventInterval = EventInterval.MINUTE, maxInterval: EventInterval = EventInterval.MONTH) -> None:
		"""
		It measures how many times a given event happened. Does not	store any data connected to the events.

		:Parameters:
		  - `name`: name of the state, it will be used in the collection name
		  - `minInterval`: smallest time interval of the measurement
		  - `maxInterval`: largest time interval of the measurement
		"""
		super().__init__(name, minInterval, maxInterval)

	def _getCollection(self,interval:EventInterval) -> pymongo.collection:
		global database
		return database[self.name+"_"+str(interval)]
	
	def onEvent(self) -> None:		
		smallestInterval = self.intervals[0]
		time = StatBase.getDatetimeForInterval(smallestInterval)

		coll = self._getCollection(smallestInterval)
		coll.update_one({"_id":time},{"$inc":{"value":1}},True)
		"""
		Call this function when the event happens
		"""
	
	def onInterval(self) -> None:
		"""
		Call this function periodically, at least as often as the second smallest interval
		"""
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
			coll = self._getCollection(EventInterval(interval.value-1))
			colltarget = self._getCollection(interval)
	
			if colltarget.count_documents({"_id":time}) == 0:
				coll.aggregate([
					{"$match":{"_id":{"$lt":currenttime,"$gte":time}}},
					{"$group":{"_id":time,"value":{"$sum":"$value"}}},
					{"$out":{"db": database.name,"coll":self.name+"_"+str(interval)}}
				])

				#if the $match is empty, no document is created
				if colltarget.count_documents({"_id":time}) == 0:
					colltarget.insert_one({"_id":time,"value":0})
	
	def getDataView(self,interval:EventInterval,startDate:datetime,endDate:datetime) -> tuple[list[datetime],list[int]]:
		"""
		Gets the collected data from the time range.
		Returns a list of keys and values as `list[datetime], list[int]`
		"""
		coll = self._getCollection(interval)
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
	"""
	A stat object for event based statistics
	"""
	def __init__(self, name: str, startEvent:str=None, endEvent:str=None, magnitudeEvent:str=None, durationEvent:str=None,
			minInterval: EventInterval = EventInterval.MINUTE, maxInterval: EventInterval = EventInterval.MONTH, expireAfterSeconds=None) -> None:
		"""
		:Parameters:
		  - `name`: name of the state, it will be used in the session collection name
		  - `startEvent` (optional): name of the start event, if provided it will be measured in
		    an :class:`EventStat` object, the events will be triggered at `onStartEvent` function call
		  - `endEvent` (optional): name of the end event, if provided it will be measured in
		    an :class:`EventStat` object, the events will be triggered at `onEndEvent` function call
		  - `magnitudeEvent` (optional): name of the magnitude event, if provided it will be measured in
		    an :class:`EventStat` object, the current count of the session is recorded at the intervals
		  - `durationEvent` (optional): name of the collection where the session durations are stored
		    if provided
		  - `minInterval`: smallest time interval of the measurement
		  - `maxInterval`: largest time interval of the measurement
		  - `expireAfterSeconds` (optional): if provided the sessions will have a TTL set with this time amount.
		    the expired sessions will not create duration events
		"""
		super().__init__(name, minInterval, maxInterval)

		if startEvent:
			self.startEvent:EventStat | None = EventStat(startEvent)
			"Optional EventStat for session start events"
			self.startEvent.intervals = self.intervals
		if endEvent:
			self.endEvent:EventStat | None = EventStat(endEvent)
			"Optional EventStat for session end events"
			self.endEvent.intervals = self.intervals
		if magnitudeEvent:
			self.magnitudeEvent:EventStat | None = EventStat(magnitudeEvent)
			"Optional EventStat for the magnitude of the session count"
		self.durationEventName = durationEvent

		if expireAfterSeconds:
			global database
			self.useTtl = True
			self._getSessionCollection().create_index({"expires":1},expireAfterSeconds=expireAfterSeconds)

	def _getSessionCollection(self):
		"""
		"""
		return database[self.name+"_SESSION"]
	
	def onStartEvent(self,id):
		"""
		Call this function when the state starts. Provide a unique id for the state, you will need to call
		`onEndEvent` with the same id when the state ends. The `id` can be any datatype that can be any type that the pymongo
		can handle
		"""
		global database

		coll = self._getSessionCollection()
		try:
			coll.insert_one({"_id":id,"created":datetime.now()})
		except pymongo.errors.DuplicateKeyError:
			self.onEndEvent(id)
			self.onStartEvent(id)
			return

		if self.startEvent:
			self.startEvent.onEvent()
	
	def onEndEvent(self,id):
		"""
		Call this function when the state ends. Provide a unique id for the state
		"""
		global database

		coll = self._getSessionCollection()
		doc = coll.find_one_and_delete({"_id":id})
		if  not doc:
			return

		if self.endEvent:
			self.endEvent.onEvent()

		if self.durationEventName:
			delta = datetime.now() - doc["created"]
			duration = delta.total_seconds()

			coll = database[self.durationEventName]
			coll.insert_one({"duration":duration,"endTime":datetime.now()})
	
	def onInterval(self) -> None:
		"""
		Call this function periodically, at least as often as the second smallest interval
		"""
		now = datetime.now(tz=None)

		if self.startEvent:
			self.startEvent.onInterval()
		if self.endEvent:
			self.endEvent.onInterval()
		
		#magnitude
		smallestInterval = self.intervals[0]
		time = StatBase.getDatetimeForInterval(smallestInterval,now) #end of the measure window

		if self.magnitudeEvent:
			sessioncoll = self._getSessionCollection()
			count = sessioncoll.count_documents({})

			coll = self.magnitudeEvent._getCollection(smallestInterval)
			coll.insert_one({"_id":time,"value":count},True)

		for i in range(1,len(self.intervals)):
			interval = self.intervals[i]
			
			#check if the result exists and create the aggregation if needed
			time = StatBase.getPrevDatetimeForInterval(interval,now) #start of the measure window
			currenttime = StatBase.getDatetimeForInterval(interval,now) #end of the measure window

			#collection to collect the data from
			coll = self.magnitudeEvent._getCollection(EventInterval(interval.value-1))
			colltarget = self.magnitudeEvent._getCollection(interval)
	
			if colltarget.count_documents({"_id":time}) == 0:
				coll.aggregate([
					{"$match":{"_id":{"$lt":currenttime,"$gte":time}}},
					{"$group":{"_id":time,"value":{"$max":"$value"}}},
					{"$out":{"db": database.name,"coll":self.magnitudeEvent+"_"+str(interval)}}
				])

				#if the $match is empty, no document is created
				if colltarget.count_documents({"_id":time}) == 0:
					colltarget.insert_one({"_id":time,"value":0})
		

