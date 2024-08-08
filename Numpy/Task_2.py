# -*- coding: utf-8 -*-
"""
Created on Tue Aug  6 10:46:11 2024

@author: Vladislav Hnatiuk
"""
import numpy as np

def print_array(arr,message):
    print(message)
    print(arr)
    print("#############")

def total_revenue(e_comm):
   # Total Revenue Function: Create a function to calculate the total revenue generated 
   # by multiplying quantity and price, and summing the result.
   return np.sum(e_comm[-2,:]*e_comm[-3,:])

def unique_users(e_comm):
    # Unique Users Function: Develop a function to determine the number of unique 
    # users who made transactions
    return np.unique(e_comm[1]) 

def most_purchased_product(e_comm):
   # Most Purchased Product Function: Implement a function to identify the most 
   # purchased product based on the quantity sold
   return e_comm[2,np.argmax(e_comm[-3])] 

def convert_price_to_int(e_comm):
    # Create a function to convert the price array from float to integer
    e_comm[4] = e_comm[4].astype(int)
    return e_comm.copy()

def check_data_type(e_comm):
    # Develop a function to check the data types of each column in the array
    return [e_comm[i,:].dtype for i in np.arange(e_comm.shape[0])]

def product_quantity_array(e_comm):
    # Product Quantity Array Function: Create a function that returns a new array with 
    # only the product_id and quantity columns.
    return e_comm[[2,3],:].copy()

def user_transaction_count(e_comm):
    # User Transaction Count Function: Implement a function to generate an array of 
    # transaction counts per user.
    return np.unique(e_comm[1], return_counts=True)

def masked_array(e_comm):
    # Masked Array Function: Create a function to generate a masked array that hides 
    # transactions where the quantity is zero.
    mask = np.repeat([[e_comm[3] == 0]], e_comm.shape[0],axis = 1)[0]
    return np.ma.masked_array(e_comm, mask=mask)

def price_increase(e_comm,percentage = 5):
    # Price Increase Function: Develop a function to increase all prices by a certain 
    # percentage (e.g., 5% increase)
    return e_comm[-2]*(1+percentage/100)

def filter_transactions(e_comm):
    # Filter Transactions Function: Implement a function to filter transactions to only 
    # include those with a quantity greater than 1
    return e_comm[:,e_comm[3] > 1].copy()

def revenue_comparison(e_comm,period1 = {'start':'2024-08-01','end':'2024-08-03'},
                       period2 = {'start':'2024-08-05','end':'2024-08-08'}):
    # Revenue Comparison Function: Create a function to compare the revenue from 
    # two different time periods
    Revenue1 = total_revenue(e_comm[:,(e_comm[-1] > period1['start']) &
                                    (e_comm[-1] <= period1['end'])])
    Revenue2 = total_revenue(e_comm[:,(e_comm[-1] > period2['start']) &
                                    (e_comm[-1] <= period2['end'])])
    if Revenue1 > Revenue2:
        return f'Revenue from period1 ({Revenue1}) is bigger than revenue from period2 ({Revenue2})'
    elif Revenue1 < Revenue2:
        return f'Revenue from period2 ({Revenue2}) is bigger than revenue from period1 ({Revenue1})'
    else:
        return 'Revenue from period1  ({Revenue1}) and period2  ({Revenue2}) is equal'

def user_transactions(e_comm,user_id = 2):
    # User Transactions Function: Create a function to extract all transactions for a 
    # specific user.
    return e_comm[:,e_comm[1] == user_id].copy()

def date_range_slicing(e_comm,start = '2024-08-05',end = '2024-08-08'):
    # Date Range Slicing Function: Develop a function to slice the dataset to include 
    # only transactions within a specific date range.
    return e_comm[:,(e_comm[-1] >= start) & (e_comm[-1] < end)].copy()

def top_products(e_comm):
    # Top Products Function: Implement a function using advanced indexing to retrieve 
    # transactions of the top 5 products by revenue.
    return e_comm[:,(e_comm[3]*e_comm[4]).argsort()[-5:]].copy()



    
# The array should include transaction_id, user_id, 
# product_id, quantity, price, and timestamp.
e_comm = np.array([np.arange(1,11), # transaction_id
                    np.random.randint(1,5,10), # user_id
                    np.random.randint(1,20,10), # product_id
                    np.random.randint(0,5,10), # quantity
                    np.random.uniform(low=10, high=50, size=(10,)), # price
                    ['2024-08-01','2024-08-02','2024-08-03','2024-08-04',
                     '2024-08-05','2024-08-06','2024-08-07','2024-08-08',
                     '2024-08-10','2024-08-11'] # timestamp
                    ], dtype=object)
print_array(e_comm, 'Generated array: ')

# Data Analysis Functions:
print_array(total_revenue(e_comm), 'Result total_revenue function:') 

print_array(unique_users(e_comm), 'Unique Users Function result:') 

print_array(most_purchased_product(e_comm),'Most Purchased Product Function result:')

print_array(convert_price_to_int(e_comm), ' Array with price converted to int:')

print_array(check_data_type(e_comm), 'Array data types:')

# Array Manipulation Functions
print_array(product_quantity_array(e_comm), 'Sub-array wtih product_id and quantity:')

print_array(user_transaction_count(e_comm), 'Transaction count per user:')

print_array(masked_array(e_comm), 'Masked array with hidden transactions where the quantity is zero:')

# Arithmetic and Comparison Functions
print_array(price_increase(e_comm), ' Array with increased price by 5%:')

print_array(filter_transactions(e_comm), 'Sub-array where quantity > 1:')

print_array(revenue_comparison(e_comm), 'Results revenue_comparison function:')

# Indexing and Slicing Functions
print_array(user_transactions(e_comm), 'Sub-array where user_id = 2:')

print_array(date_range_slicing(e_comm), 'Sub-array where date between 2024-08-05 and 2024-08-08')

print_array(top_products(e_comm), 'Array with top 5 products by revenue')

