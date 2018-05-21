# Pierrot Musumbu Dibwe
# Implement a DNA sequence trimming MapReduce query

import MapReduce
import sys

"""
Consider a set of key-value pairs where each key is sequence id and each value is a string of nucleotides, e.g., GCTTCCGAAATGCTCGAA....

Write a MapReduce query to remove the last 10 characters from each string of nucleotides, then remove any duplicates generated.

Map Input
Each input record is a 2 element list [sequence id, nucleotides] where sequence id is a string representing a unique identifier for the sequence and nucleotides is a string representing a sequence of nucleotides

Reduce Output
The output from the reduce function should be the unique trimmed nucleotide strings.

To execute the code on Linux I run:
/assignment3$ python unique_trims.py data/dna.json > unique_trims.txt

I export the output into a file. 

"""

mr = MapReduce.MapReduce()

def mapper(record):
    # key: document_id
    # value: text
    nucleotides = record[1][:-10]
    mr.emit_intermediate(nucleotides, 1)

def reducer(nucleotides, full_record):
    # key: person_id
    # value: list of friends
    mr.emit(nucleotides)
   

# Do not modify below this line
# =============================
if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)