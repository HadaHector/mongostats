from datetime import datetime,timedelta
import pymongo as pymongo
from enum import IntEnum
import pymongo.collection
import typing

dbclient = None
database = None

def initialize_connection(mongoclient:pymongo.MongoClient,dbname:str):
    """
    This function initializes the database connection for the statistics
    """
    global dbclient, database
    dbclient = mongoclient
    database = mongoclient[dbname]

# Decorator for error handling
def handle_database_errors(func):
    def wrapper(*args, **kwargs):
        if not dbclient:
            raise ConfigError("The database connection is not initialized")
        return func(*args, **kwargs)
    return wrapper

class ConfigError(Exception):
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
    """
    Base class for stat measurement, has no own functionality
    """
    def __init__(self,name:str,min_interval:EventInterval=EventInterval.MINUTE,
                 max_interval:EventInterval=EventInterval.MONTH) -> None:
        super()
        self.name = name
        self.intervals = []
        for i in range(min_interval.value,max_interval.value+1):
            self.intervals.append(EventInterval(i))
        
        if not len(self.intervals):
            raise ConfigError("There are no intervals in this stat")
    
    @staticmethod
    def get_datetime_for_interval(interval:EventInterval,
                                  time=None) -> datetime:
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
    
    @staticmethod
    def get_shifted_interval(interval:EventInterval,time,amount) -> datetime:
        if interval == EventInterval.MONTH:
            for _ in range(abs(amount)):
                if amount < 0 and time.month == 1:
                    time = time.replace(year=time.year-1,month=12)
                if amount > 0 and time.month == 12:
                    time = time.replace(year=time.year+1,month=1)
                else:
                    time = time.replace(month=time.month+(1 if amount>0 else -1))
        elif interval == EventInterval.DAY:
            time = time + timedelta(days=amount)
        elif interval == EventInterval.HOUR:
            time = time + timedelta(hours=amount)
        elif interval == EventInterval.MINUTE:
            time = time + timedelta(minutes=amount)
        elif interval == EventInterval.SECOND:
            time = time + timedelta(seconds=amount)
        return time

    @staticmethod
    def get_prev_interval(interval:EventInterval,time) -> datetime:
        return StatBase.get_shifted_interval(interval,time,-1)
    
    @staticmethod
    def get_next_interval(interval:EventInterval,time) -> datetime:
        return StatBase.get_shifted_interval(interval,time,1)


class EventStat(StatBase):
    """
    Basic event based statistics.
    """
    def __init__(self, name: str, 
                min_interval: EventInterval = EventInterval.MINUTE,
                max_interval: EventInterval = EventInterval.MONTH) -> None:
        """
        It measures how many times a given event happened. Does not
        store any data connected to the events.

        :Parameters:
          - `name`: name of the state, it will be used in the collection
            name
          - `min_interval`: smallest time interval of the measurement
          - `max_interval`: largest time interval of the measurement
        """
        super().__init__(name, min_interval, max_interval)

    def _get_collection(self,
                        interval:EventInterval) -> pymongo.collection:
        global database
        return database[self.name+"_"+str(interval)]
    
    @handle_database_errors
    def on_event(self) -> None:
        smallestInterval = self.intervals[0]
        time = StatBase.get_datetime_for_interval(smallestInterval)

        coll = self._get_collection(smallestInterval)
        coll.update_one({"_id":time},{"$inc":{"value":1}},True)
        """
        Call this function when the event happens
        """
    
    @handle_database_errors
    def on_interval(self) -> None:
        """
        Call this function periodically, at least as often as the second
        smallest interval
        """
        global database
        #let's assume this is called every second smallest interval
        if len(self.intervals) < 2:
            return
        
        now = datetime.now(tz=None)

        for i in range(1,len(self.intervals)):
            interval = self.intervals[i]
            
            #check if the result exists and create the aggregation if needed
            current_time = StatBase.get_datetime_for_interval(interval,now) 
            #end of the measure window
            time = StatBase.get_prev_interval(interval,current_time) 
            #start of the measure window

            #collection to collect the data from
            coll = self._get_collection(EventInterval(interval.value-1))
            coll_target = self._get_collection(interval)
    
            if coll_target.count_documents({"_id":time}) == 0:
                coll.aggregate([
                    {"$match":{"_id":{"$lt":current_time,"$gte":time}}},
                    {"$group":{"_id":time,"value":{"$sum":"$value"}}},
                    {"$merge":{
                        "out":self.name+"_"+str(interval)
                    }}
                ])

                #if the $match is empty, no document is created
                if coll_target.count_documents({"_id":time}) == 0:
                    coll_target.insert_one({"_id":time,"value":0})
    
    @handle_database_errors
    def get_data_view(self,interval:EventInterval,start_date:datetime,
                      end_date:datetime) -> typing.Tuple[typing.List[datetime],typing.List[int]]:
        """
        Gets the collected data from the time range.
        Returns a list of keys and values as `list[datetime], list[int]`
        """
        coll = self._get_collection(interval)
        documents = list(coll.find(
            filter={"_id":{"$gte":start_date,"$lte":end_date}},
            sort=[('_id', pymongo.ASCENDING)]
        ))

        keys = []
        values = []

        key = StatBase.get_datetime_for_interval(interval,start_date)
        key = StatBase.get_prev_interval(interval,key) 
        doc_idx = 0
        while True:
            keys.append(key)
            if doc_idx<len(documents) and documents[doc_idx]["_id"] == key:
                values.append(documents[doc_idx]["value"])
                doc_idx += 1
            else:
                values.append(0)
            key = StatBase.get_next_interval(interval,key)
            if key > end_date:
                break
        
        return keys,values


class StateStat(StatBase):
    """
    A stat object for event based statistics
    """
    @handle_database_errors
    def __init__(
            self, name: str, start_event:str=None, end_event:str=None,
            magnitude_event:str=None, duration_event:str=None,
            unique_start_event:str=None,
            min_interval: EventInterval = EventInterval.MINUTE,
            max_interval: EventInterval = EventInterval.MONTH,
            expire_after_seconds=None) -> None:
        """
        :Parameters:
          - `name`: name of the state, it will be used in the session
            collection name
          - `start_event` (optional): name of the start event, if provided it
            will be measured in an :class:`EventStat` object, the events will
            be triggered at `on_start_event` function call
          - `end_event` (optional): name of the end event, if provided it will
            be measured in an :class:`EventStat` object, the events will be
            triggered at `on_end_event` function call
          - `magnitude_event` (optional): name of the magnitude event, if
            provided it will be measured in an :class:`EventStat` object, the
            current count of the session is recorded at the intervals
          - `unique_start_event` (optional): name of the unique start event,
            if provided it will be measured in an :class:`EventStat` object,
            the unique ids that has session in the interval measured
          - `duration_event` (optional): name of the collection where the
            session durations are stored if provided
          - `min_interval`: smallest time interval of the measurement
          - `max_interval`: largest time interval of the measurement
          - `expire_after_seconds` (optional): if provided the sessions will
            have a TTL set with this time amount. The expired sessions will not
            create duration events
        """
        super().__init__(name, min_interval, max_interval)

        self.start_event:EventStat | None = None
        "Optional EventStat for session start events"
        self.end_event:EventStat | None = None
        "Optional EventStat for session end events"
        self.magnitude_event:EventStat | None = None
        "Optional EventStat for the magnitude of the session count"
        self.unique_start_event:EventStat | None = None
        "Optional EventStat for the unique session activity"

        if start_event:
            self.start_event = EventStat(start_event)
            self.start_event.intervals = self.intervals
        if end_event:
            self.end_event = EventStat(end_event)
            self.end_event.intervals = self.intervals
        if magnitude_event:
            self.magnitude_event:EventStat | None = EventStat(magnitude_event)
            self.magnitude_event.intervals = self.intervals
        if unique_start_event:
            self.unique_start_event:EventStat | None = EventStat(unique_start_event)
            self.unique_start_event.intervals = self.intervals

        self.duration_event_name = duration_event

        if expire_after_seconds:
            global database
            self.use_ttl = True
            self._get_session_collection().create_index(
                {"created":1},
                expireAfterSeconds=expire_after_seconds)

    def _get_session_collection(self):
        return database[self.name+"_SESSION"]
    
    @handle_database_errors
    def on_start_event(self,id):
        """
        Call this function when the state starts. Provide a unique id for the
        state, you will need to call `on_end_event` with the same id when the
        state ends. The `id` can be any datatype that can be any type that the
        pymongo can handle
        """
        global database

        try:
            coll = self._get_session_collection()
            coll.insert_one({"_id":id,"created":datetime.now()})
        except pymongo.DuplicateKeyError:
            self.on_end_event(id)
            self.on_start_event(id)
            return
        
        if self.unique_start_event:
            for i in range(1,len(self.intervals)):
                interval = self.intervals[i]
                coll = database[self.name+"_UNIQUE_"+interval]
                doc = {"_id":id}
                coll.replace_one(doc,doc,upsert=True)

        if self.start_event:
            self.start_event.on_event()
    
    @handle_database_errors
    def on_end_event(self,id):
        """
        Call this function when the state ends. Provide a unique id for the
        state
        """
        global database

        coll = self._get_session_collection()
        doc = coll.find_one_and_delete({"_id":id})
        if  not doc:
            return

        if self.end_event:
            self.end_event.on_event()

        if self.duration_event_name:
            delta = datetime.now() - doc["created"]
            duration = delta.total_seconds()

            coll = database[self.duration_event_name]
            coll.insert_one({"duration":duration,"endTime":datetime.now()})
    
    @handle_database_errors
    def on_interval(self) -> None:
        """
        Call this function periodically, at least as often as the second
        smallest interval
        """
        now = datetime.now(tz=None)

        if self.start_event:
            self.start_event.on_interval()
        if self.end_event:
            self.end_event.on_interval()
        
        #magnitude
        smallest_interval = self.intervals[0]
        time = StatBase.get_datetime_for_interval(smallest_interval,now)
        #end of the measure window

        if self.magnitude_event:
            sessioncoll = self._get_session_collection()
            count = sessioncoll.count_documents({})

            coll = self.magnitude_event._get_collection(smallest_interval)
            coll.insert_one({"_id":time,"value":count})

        for i in range(1,len(self.intervals)):
            interval = self.intervals[i]
            
            #check if the result exists and create the aggregation if needed
            currenttime = StatBase.get_datetime_for_interval(interval,now)
            #end of the measure window
            time = StatBase.get_prev_interval(interval,currenttime)
            #start of the measure window

            if self.magnitude_event:
                #collection to collect the data from
                coll = self.magnitude_event._get_collection(
                    EventInterval(interval.value-1))
                colltarget = self.magnitude_event._get_collection(interval)
        
                if colltarget.count_documents({"_id":time}) == 0:
                    coll.aggregate([
                        {"$match":{"_id":{"$lt":currenttime,"$gte":time}}},
                        {"$group":{"_id":time,"value":{"$max":"$value"}}},
                        {"$merge":{
                            "into":self.magnitude_event.name+"_"+str(interval)
                        }}
                    ])

                    #if the $match is empty, no document is created
                    if colltarget.count_documents({"_id":time}) == 0:
                        colltarget.insert_one({"_id":time,"value":0})
        
        smallest_rounded = StatBase.get_datetime_for_interval(interval,now)
        for i in range(len(self.unique_start_event.intervals)):
            interval = self.unique_start_event.intervals[i]
            currenttime = StatBase.get_datetime_for_interval(interval,now)

            #only do the larger intervals when needed
            if smallest_rounded == currenttime:
                coll = self.unique_start_event._get_collection(interval)
                measurecoll = database[self.name+"_UNIQUE_"+interval]
                
                #get the count of ids on the current interval and save it
                count = measurecoll.count_documents()
                coll.insert_one({"_id":currenttime,"value":count})

                #overwrite the current ids with the currently online players ids
                # sessions -> unique_interval
                self._get_session_collection().aggregate([
                    {"$project":{"_id":1}},
                    {"$out":self.name+"_UNIQUE_"+interval}
                ])


        
 