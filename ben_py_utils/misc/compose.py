from functools import reduce
import pandas as pd
import numpy as np
from os.path import abspath, join, isdir,dirname


def composite_function(*func):
    ''' Compose a function
    https://www.geeksforgeeks.org/function-composition-in-python/#:~:text=Function%20composition%20is%20the%20way,second%20function%20and%20so%20on.
    '''
    def compose(f, g):
        return lambda x : f(g(x))

    return reduce(compose, func, lambda x : x)

def apply_n_times(fun, n, *args, **kws):
    '''Apply a function n times
	'''

    fun_power_n = [fun for i in range(n)]

    return composite_function ( *fun_power_n  )( *args , **kws) #need to unpack fun_power_n, which is a list

def move_up_n_time(path, n):
	''' Move up n times in a directory, starting at path
	'''
	new_dir = apply_n_times( dirname, n, path)


	if new_dir == '/':
		print('Warning, moved up to the root')

	return new_dir
