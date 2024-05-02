import testsetup as setup
import main as stat

test_stat:stat.StateStat = setup.stat["users"]

test_stat.on_start_event("sanyi")
test_stat.on_end_event("bela")
test_stat.on_interval()