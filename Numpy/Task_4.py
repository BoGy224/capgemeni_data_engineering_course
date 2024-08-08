# -*- coding: utf-8 -*-
"""
Created on Wed Aug  7 11:52:10 2024

@author: Vladislav Hnatiuk
"""

import numpy as np
import os

def print_array(arr,message):
    print(message)
    print(arr)
    print("#############")

def save(arr,filename = 'arr'):
    # Save Function: Create a function to save the array to a text file, a CSV file, and a 
    # binary format (.npy or .npz).
    np.savetxt(filename + '.txt',arr)
    np.savetxt(filename + '.csv', arr, delimiter=",")
    np.save(filename, arr)
    np.savez(filename, arr)

def load(filename):
    # Load Function: Develop a function to load the array from each of the saved formats 
    # back into NumPy arrays
    fname, file_extension = os.path.splitext(filename)
    if file_extension == '.txt':
        return np.loadtxt(filename).astype(np.int16)
    elif file_extension == '.csv':
        return np.loadtxt(filename,delimiter=',').astype(np.int16)
    elif file_extension == '.npy':
        return np.load(filename)
    else:
        return np.load(filename)['arr_0']
    
def summation(arr,axis = None):
    # Summation Function: Create a function to compute the summation of all 
    # elements
    return np.sum(arr,axis)

def mean(arr,axis = None):
    # Mean Function: Develop a function to calculate the mean of the array
    return np.mean(arr,axis)

def median(arr,axis = None):
    # Median Function: Implement a function to find the median of the arra
    return np.median(arr,axis)

def standard_deviation(arr,axis = None):
    # Standard Deviation Function: Construct a function to calculate the standard 
    # deviation of the array
    return np.std(arr,axis)

def axis_based_aggregate(arr,axis = 0):
    # Axis-Based Aggregate Functions: Create functions to apply these aggregate 
    # operations along different axes (row-wise and column-wise).
    return {'sum' : summation(arr,axis),
            'mean' : mean(arr,axis),
            'median' : median(arr,axis),
            'std' : standard_deviation(arr,axis)}

# 1. Array Creation
arr = np.random.randint(10,size = (10,10),dtype = np.int16)
print_array(arr, 'Genereted array:')

# 2. Data I/O Functions
save(arr,filename = 'arr') #Save Function

arr_txt = load('arr.txt') #Load Function
print_array(arr_txt, ' Array from txt file')

arr_csv = load('arr.csv')
print_array(arr_csv, ' Array from csv file')

arr_npy = load('arr.npy')
print_array(arr_npy, ' Array from npy file')

arr_npz = load('arr.npz')
print_array(arr_npz, ' Array from npz file')

assert np.array_equal(arr,arr_txt), "Error: imported array from txt file is not equal with original array"
assert np.array_equal(arr,arr_csv), "Error: imported array from csv file is not equal with original array"
assert np.array_equal(arr,arr_npy), "Error: imported array from npy file is not equal with original array"
assert np.array_equal(arr,arr_npz), "Error: imported array from npz file is not equal with original array"

# 3. Aggregate Functions:
print_array(summation(arr),'Sum of all array elements: ') # Summation Function

print_array(mean(arr),'Mean of the generated array: ') # Mean Function

print_array(median(arr),'Median of the generated array: ')# Median Function

print_array(standard_deviation(arr),
            'Standart deviation of the generated array: ') # Standard Deviation Function

# Axis-Based Aggregate Functions
print_array(axis_based_aggregate(arr,axis = 0),
            'Sum,mean,median and std of the generated array calculated by axis = 0')

print_array(axis_based_aggregate(arr,axis = 1),
            'Sum,mean,median and std of the generated array calculated by axis = 1')








