# Pierrot Musumbu Dibwe
# Implement a social network MapReduce query

import MapReduce
import sys

"""
Consider a simple social network dataset consisting of a set of key-value pairs (person, friend) representing a friend relationship between two people. Describe a MapReduce algorithm to count the number of friends for each person.

Map Input
Each input record is a 2 element list [personA, personB] where personA is a string representing the name of a person and personB is a string representing the name of one of personA's friends. Note that it may or may not be the case that the personA is a friend of personB.

Reduce Output
The output should be a pair (person, friend_count) where person is a string and friend_count is an integer indicating the number of friends associated with person.

To execute the code on Linux I run:
/assignment3$ python friend_count.py data/friends.json > friend_count.txt

I export the output into a file. 

"""

mr = MapReduce.MapReduce()

def mapper(record):
    # key: document_id
    # value: text
    friends = tuple(sorted(record))
    mr.emit_intermediate(friends, 1)

def reducer(friends, friends_list):
    # key: person_id
    # value: list of friends
    if len(friends_list) == 1:
        mr.emit((friends[0], friends[1]))
        mr.emit((friends[1], friends[0]))
   

# Do not modify below this line
# =============================
if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)