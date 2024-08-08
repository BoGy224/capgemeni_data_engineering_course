# -*- coding: utf-8 -*-
"""
Created on Wed Aug  7 10:54:59 2024

@author: Vladislav Hnatiuk
"""

import numpy as np

def print_array(arr,message):
    print(message)
    print(arr)
    print("#############")

def transpose(arr):
    # Transpose Function: Create a function to transpose the array and return the result.
    return arr.T.copy()

def reshape(arr):
    # Reshape Function: Develop a function to reshape the array into a new 
    # configuration (e.g., from a 6x6 matrix to a 3x12 matrix) and return the reshaped 
    # array.
    return arr.reshape((3,12)).copy()

def split(arr,chunks = 2,axis = 0):
    # Split Function: Implement a function that splits the array into multiple sub-arrays 
    # along a specified axis and returns the sub-arrays
    return np.split(arr,chunks,axis)

def combine(arrays,axis = 0):
    # Combine Function: Construct a function that combines several arrays into one and 
    # returns the combined array
    return np.concatenate(arrays, axis=axis)    

# 1. Array Creation
arr = np.random.randint(10,size = (6,6),dtype = np.int16)
print_array(arr,'Genereted array:')

# 2. Array Manipulation Functions:
arrT = transpose(arr) # Transpose Function
print_array(arrT,' Transpose array:')

arr_reshape = reshape(arr) #Reshape Function
print_array(arr_reshape,'Reshaped array: ')

arrays = split(arr) #Split Function
print_array(arrays,'Splited arrays: ')

combined_arr = combine(arrays) #Combine Function
print_array(combined_arr,'Combined array: ')

assert arrT.shape == (6, 6), "Error: 'transpose' function return wrong result"
assert arr_reshape.shape == (3, 12), "Error: 'reshape' function return wrong result"
assert len(arrays) == 2, "Error: 'split' function return wrong result"
assert combined_arr.shape == arr.shape, "Error: 'combine' function return wrong result"


