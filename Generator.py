'''
Реализовать метод super_range, получить из объекта длину для итерирования используя логику генератора. 
Не создавать массив, а генерировать значение каждый раз при вызове.
'''

def super_range(i):
    number = 0
    while number < i:
        yield number
        number += 1
        
for number in super_range(100):
    print(number)
