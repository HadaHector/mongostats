import testsetup as setup
import main as stat

test_stat:stat.EventStat = setup.stats["testevent"]

test_stat.on_event()
test_stat.on_interval()