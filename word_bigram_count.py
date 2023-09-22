from mrjob.job import MRJob
import re

WORD_RE = re.compile(r"[\w']+")

class WordBigramCount(MRJob):

    def mapper(self, _, line):
        
        w = WORD_RE.findall(line.lower())  
       
        for i in range(len(w) - 1):
            bigram = f"{w[i]},{w[i+1]}"
            yield bigram, 1

    def reducer(self, key, values):
        
        yield key, sum(values)

if __name__ == '__main__':
    WordBigramCount.run()
#python word_bigram_count.py input3.txt > output3.txt
