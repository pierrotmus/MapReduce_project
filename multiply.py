# Pierrot Musumbu Dibwe
# Implement a DNA sequence trimming MapReduce query

import MapReduce
import sys

"""
Assume you have two matrices A and B in a sparse matrix format, where each record is of the form i, j, value. Design a MapReduce algorithm to compute the matrix multiplication A x B
Map Input

The input to the map function will be a row of a matrix represented as a list. Each list will be of the form [matrix, i, j, value] where matrix is a string and i, j, and value are integers.

The first item, matrix, is a string that identifies which matrix the record originates from. This field has two possible values: "a" indicates that the record is from matrix A and "b" indicates that the record is from matrix B.
Reduce Output

The output from the reduce function will also be a row of the result matrix represented as a tuple. Each tuple will be of the form (i, j, value) where each element is an integer.

To execute the code on Linux I run:
/assignment3$ python multiply.py data/matrix.json > multiply.txt

I export the output into a file. 

"""

mr = MapReduce.MapReduce()

def mapper(record):
    
    matrix, row, col, value = record
    
    for n in range(5):
        if matrix == 'a':
            result = (row,n)
            left_matrix = 'left'
            index = col
        else:
            result = (n,col)
            left_matrix = 'right'
            index = row
        mr.emit_intermediate( result, (left_matrix ,index, value) )
            

def reducer(key, list_of_values):
    
    lm_result = [ (item[1],item[2]) for item in list_of_values if item[0] == 'left' ]
    rm_result = [ (item[1],item[2]) for item in list_of_values if item[0] == 'right' ]

    result = 0

    for element_a in lm_result:
        for element_b in rm_result:
            if element_a[0] == element_b[0] :
                result += element_a[1] * element_b[1]

    mr.emit(( key[0], key[1], result ))
   

# Do not modify below this line
# =============================
if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)