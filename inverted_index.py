from mrjob.job import MRJob
import re

WORD_RE = re.compile(r"[\w']+")

class InvertedIndex(MRJob):

    def mapper(self, _, line):
        # Split the line into document ID and content
        doc_id, content = line.split(": ", 1)
        # Tokenize the content into words
        words = WORD_RE.findall(content.lower())  # Convert to lowercase for case-insensitive indexing
        # Emit each word with the document ID it appears in
        for word in words:
            yield word, doc_id

    def reducer(self, key, values):
        # Collect the document IDs for each word
        doc_ids = list(set(values))  # Deduplicate document IDs
        yield key, doc_ids

if __name__ == '__main__':
    InvertedIndex.run()

#python inverted_index.py input4.txt > output4.txt
