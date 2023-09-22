from mrjob.job import MRJob
import re

# List of stopwords
STOPWORDS = {'the', 'and', 'of', 'a', 'to', 'in', 'is', 'it'}

WORD_RE = re.compile(r"[\w']+")

class WordCountNoStopwords(MRJob):

    def mapper(self, _, line):
        # Tokenize the line into words
        words = WORD_RE.findall(line.lower())  # Convert to lowercase for case-insensitive counting
        # Emit each non-stopword as a key with a value of 1
        for word in words:
            if word not in STOPWORDS:
                yield word, 1

    def reducer(self, key, values):
        # Sum up the values (which are all 1s) to get the count of non-stopwords
        yield key, sum(values)

if __name__ == '__main__':
    WordCountNoStopwords.run()
