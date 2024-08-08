# -*- coding: utf-8 -*-
"""
Created on Tue Aug  6 10:08:58 2024

@author: Vladislav Hnatiuk
"""

import numpy as np

def print_array(arr,message):
    print(message)
    print(arr)
    print("#############")

# 1. Array Creation:
dim1 = np.arange(1,11)
dim2 = np.arange(1,10).reshape((3,3))

# 2. Basic Operations:
    #   Indexing and Slicing:
print_array(dim1[2],'Print the third element of the one-dimensional array')
print_array(dim2[0:2,0:2], 'Print the first two rows and columns of the two-dimensional array')

    # Basic Arithmetic:
dim1 += 5
print_array(dim1,'Add 5 to each element of the one-dimensional array and print the result')
dim2 *= 2
print_array(dim2,'Multiply each element of the two-dimensional array by 2 and print the result.')
