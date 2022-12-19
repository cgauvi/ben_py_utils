
import time
import numpy as np

from ben_py_utils.misc.profiler import time_function

 


def test_timed_function():

    @time_function
    def fun_to_time(sleep_time_secs: int):
        time.sleep(sleep_time_secs)
        return True

    sleep_time_secs = 1
    result, time_to_run = fun_to_time(sleep_time_secs = sleep_time_secs)

    assert result
    assert abs(time_to_run - sleep_time_secs) < 10**-1, f"{time_to_run} vs {sleep_time_secs}"
 
