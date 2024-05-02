import testsetup as setup
import mongostats as stat

test_stat:stat.EventStat = setup.stats["testevent"]

test_stat.on_event()
test_stat.on_interval()