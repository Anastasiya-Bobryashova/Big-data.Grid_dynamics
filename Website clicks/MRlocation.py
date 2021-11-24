from mrjob.job import MRJob
from requests import get
from re import search

class MRlocation(MRJob):

    Link = 'https://ip2c.org/?ip='
    
    def mapper(self, _, line):
        IP = search(r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})', line).group(1) 
        full_info = get(MRlocation.Link + IP).text
        location = full_info.split(';')[-1]
        yield location, 1
        
    def reducer(self, location, values):
        yield (location, sum(values))
       
if __name__ == '__main__':
     MRlocation.run()