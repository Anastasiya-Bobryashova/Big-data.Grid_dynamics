from mrjob.job import MRJob
from re import compile, findall

IP = compile(r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})')    

class MRaggregate(MRJob):

    def mapper(self, _, line):
        for word in findall(IP, line):
            yield (str(word), 1)  

    def combiner(self, word, counts):
        yield (word, sum(counts))
        
    def reducer(self, key, values):
        count = sum(values)
        if count > 1:    
            yield key, count

if __name__ == '__main__':
     MRaggregate.run()