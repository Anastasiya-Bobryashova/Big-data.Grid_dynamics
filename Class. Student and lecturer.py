'''
Нужно описать при помощи классов следующие сущности:
Студент - имеет имя, возраст, пол, номер класса в котором обучается, название специальности на которую обучается;
Преподаватель - имеет имя, возраст, пол, наименование преподаваемого предмета, стаж работы;
И студент и преподаватель имеют метод think, в результате которого можно получить случайное число и метод speak(t), который увеличивает t на 10. 
В случае преподавателя метод speak увеличивает t на 10, а так же прибавляет стаж работы преподавателя.
Так же студент имеет метод sum(a,b), который складывает 2 числа. 
Преподаватель имеет метод answer(v), в результате которого возвращает v возведенное в 10-ю степень.
'''

from random import random

class Person():    
    
    def __init__(self, name, age, genre):
        self.name = name
        self.age = age
        self.genre = genre
        
    def think(self):
        return random()
    
    def speak(self, t):
        return t + 10
        
class Student(Person):
    
    def __init__(self, name, age, genre, class_number, specialty):
        super().__init__(name, age, genre)
        self.class_number = class_number
        self.specialty = specialty
        
    def sum(self, a, b):
        return a + b

class Lecture(Person):
    
    def __init__(self,  name, age, genre, course_taught, work_experience):
        super().__init__(name, age, genre)
        self.course_taught = course_taught
        self.work_experience = work_experience
    
    def speak(self, t):
        speak = super().speak(t)
        return speak + self.work_experience

    def answer(self, v):
        return pow(v, 10)
    
student_1 = Student('Nastya', 18, 'female', 2, 'BigData')
student_2 = Student('Katya', 19, 'female', 2, 'Web')

teacher_1 = Lecture('Kate', 27, 'female', 'English', 5)
teacher_2 = Lecture('Oleg', 29, 'male', 'ML', 6)

print(student_2.think())
print(student_2.speak(8))
print(student_2.sum(13, 4))

print()

print(teacher_2.think())
print(teacher_2.speak(11))
print(teacher_2.answer(2))