from mrjob.job import MRJob
from mrjob.job import MRJob
import re

WORD_RE = re.compile(r"[\w']+")

class UniqueWordCount(MRJob):

    def mapper(self, _, line):
        
        words = WORD_RE.findall(line.lower()) 
       
        for word in words:
            yield word, 1

    def reducer(self, key, values):
        
        yield key, sum(values)

if __name__ == '__main__':
    UniqueWordCount.run()
#python unique_word_count.py input.txt > output.txt