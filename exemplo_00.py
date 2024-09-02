from timer_decorator import time_measure_decorator
import time

@time_measure_decorator
def soma(x,y):
    time.sleep(2)
    return x+y

soma(2,3)
soma(2,7)