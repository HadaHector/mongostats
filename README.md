This module serves a simple purpose, to measure simple event based statistics. It only uses a MongoDB database to store all data.
It will measure the data on multiple scales (month,day,hour,minute,second), these are configurablable on a stat basis.

# Features
## Event based statistics
You just create a statistics object, and call on_event() when needed:
```py
stat = mongostats.EventStat("your_stat_name")

#when the event happens
stat.on_event()
```

## Session based statistics
This is a more complex measurement, it tracks unique sessions by id. It can produce measurements how many session is active at the same time, and how long the sessions were.
Also it can create event statistics of the session start and end.
```py
#for example if you want to measure how much the users are online
users_stat = mongostats.StateStat('users',start_event="login",end_event="logout",magnitude_event="CCU",
                                  duration_event="sessionLength")

#at login
users_stat.on_start_event("unique_userid")
#at logout
users_stat.on_end_event("unique_userid")
```
# Usage
1. You need to provide a pymong.MongoClient object as this module will not handle the creation and closure of the db connection. You can choose the database to use.
Call `mongostats.initialize_connection(client,"dbname")`.
2. Create your staticstics objects.
3. Call the `on_interval()` function on your statistics objects with the required frequency. For session based stats you need to call it the smallest scale of the stat,
for event based statistics the needed frequency is only the second smallest scale. 
For example: if you measure an event on minute basis, you only need to call it every hour, however if it is a session based you need to call it every minute
4. Get the data from the stats when you need, you can use the `get_data_view` function for this
Originally this module was planned to be used from an AWS lambda function.

