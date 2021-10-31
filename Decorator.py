'''
Сделать декоратор который может считать время выполнения функции и выводить его в консоль.
Нужно сделать возможным передать min=True/False, что позволит выводить время в минутах в случае True и в секундах в случае False.
'''

import time

def decorate(min = False):
    def timeit(func):
        def wrapper(*args, **kwargs):
            start_func = time.time()
            result = func(*args, **kwargs)
            end_func = time.time()
            if min:
                print(f'Execution time in minutes minutes: {(end_func - start_func)/60}')
            else:
                print(f'Execution time in seconds: {end_func - start_func}') 
            return result
        return wrapper
    return timeit
    
    
@decorate()
def parity_check(n):
    S = []
    for i in range (n):
        if i % 2 == 0:
            S.append(i)
    return len(S)

n = 2*(10**5)
parity_check(n)


@decorate(True)
def checking_for_positivity (s):
    S = []
    for i in range (-1000, m, 5):
        if i >  0:
            S.append(i)
    return len(S)

m = 3*(10**4)
checking_for_positivity(m)