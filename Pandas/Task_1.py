# -*- coding: utf-8 -*-
"""
Created on Tue Aug 13 12:39:42 2024

@author: Vladyslav Hnatiuk
"""

import pandas as pd
import os 
#data = pd.read_csv('D:\capgemeni\capgemeni_data_engineering_course\Pandas\AB_NYC_2019.csv')
def print_dataframe_info(data, message=""):
    print(message)
    print(data.info())
    print(data.isnull().sum())


os.chdir(os.path.dirname(__file__))

# 1. Data Loading and Initial Inspection

# Load the New York City Airbnb Open Data dataset into DataFram
data = pd.read_csv('AB_NYC_2019.csv')

# Inspect the first few rows of the dataset using the head() method to understand its 
# structure
print(data.head())
# Retrieve basic information about the dataset, such as the number of entries, column 
# data types, and memory usage, using the info() method
print(data.info())

print_dataframe_info(data,'DataFrame info before cleaning:')

# 2. Handling Missing Values:
# Identify columns with missing values and count the number of missing entries per 
# column.
print(data.isnull().sum())

# Handle missing values in the name, host_name, and last_review columns:
data['name'].fillna("Unknown",inplace = True)
data['host_name'].fillna("Unknown",inplace = True)
data['last_review'].fillna(pd.NaT,inplace = True)

# 3. Data Transformation
# Categorize Listings by Price Range:
data['price_category'] = pd.cut(data['price'], bins=[0, 100, 300, float('Inf')],
                                labels=['Low', 'Medium', 'High'],include_lowest=True)

# Create a length_of_stay_category column
data['length_of_stay_category'] = pd.cut(data['minimum_nights'],bins=[0, 3, 14, float('Inf')],
                                labels=['short-term', 'medium-term', 'long-term'])

# 4. Data Validation:
data = data[data['price'] > 0]
print_dataframe_info(data,'DataFrame info after cleaning:')


data.to_csv('cleaned_airbnb_data.csv')


