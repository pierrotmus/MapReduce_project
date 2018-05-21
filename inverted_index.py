# Pierrot Musumbu Dibwe
# Create an Inverted index using MapReduce

import MapReduce
import sys

"""
The task is: Given a set of documents, an inverted index is a dictionary where each word is associated 
with a list of the document identifiers in which that word appears.

Mapper Input
The input is a 2-element list: [document_id, text], where document_id is a string representing a document identifier and text is a string representing the text of the document. The document text may have words 
in upper or lower case and may contain punctuation. You should treat each token as if it was a valid word; that is, you can just use value.split() to tokenize the string.

Reducer Output
The output should be a (word, document ID list) tuple where word is a String and document ID list is a list of Strings.

To execute the code on Linux I run:
/assignment3$ python inverted_index.py data/books.json > inverted_index.txt

I export the output into a file. 

"""

mr = MapReduce.MapReduce()

def mapper(record):
    # key: document_id
    # value: text
    key = record[0]
    value = record[1]
    words = value.split()
    for w in words:
        mr.emit_intermediate(w, key)

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    mr.emit((key, list(set(list_of_values))))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)