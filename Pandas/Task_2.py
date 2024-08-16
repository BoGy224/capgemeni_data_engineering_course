# -*- coding: utf-8 -*-
"""
Created on Wed Aug 14 10:53:29 2024

@author: Vladyslav Hnatiuk
"""

import pandas as pd
import os 
#data = pd.read_csv('D:\capgemeni\capgemeni_data_engineering_course\Pandas\cleaned_airbnb_data.csv')

def print_grouped_data(data, message=""):
    print(message)
    print(data)

os.chdir(os.path.dirname(__file__))
data = pd.read_csv('cleaned_airbnb_data.csv')

# 1. Data Selection and Filtering

# Filter the dataset to include only listings in specific neighborhoods (e.g., Manhattan, 
# Brooklyn).
filtered_data = data[data['neighbourhood_group'].isin(['Manhattan', 'Brooklyn'])]

# Further filter the dataset to include only listings with a price greater than $100 and a 
# number_of_reviews greater than 10
filtered_data = data[(data['price'] > 100) & (data['number_of_reviews'] > 10)]

# Select columns of interest such as neighbourhood_group, price, 
# minimum_nights, number_of_reviews, price_category and availability_365 for 
# further analysi
filtered_data = filtered_data.loc[:,['neighbourhood_group', 'price',
                                     'minimum_nights', 'number_of_reviews',
                                     'price_category','availability_365']]


# 2. Aggregation and Grouping
# Calculate the average price and minimum_nights for each group.

agg_data = filtered_data.groupby(["neighbourhood_group","price_category"])[["price", "minimum_nights",
                                                               "number_of_reviews", "availability_365"]].mean()
print_grouped_data(agg_data,'Aggregation data:')

# 3. Data Sorting and Ranking
# Sort the data by price in descending order and by number_of_reviews in ascending 
# order.
print_grouped_data(filtered_data.sort_values(['price','number_of_reviews'],ascending = [0,1]))

# Create a ranking of neighborhoods based on the total number of listings and the 
# average price.
neighborhood_rank = data.groupby('neighbourhood_group').agg({
        'price': 'mean',
        'neighbourhood_group': 'count'
    }).rename(columns={'neighbourhood_group': 'total_listings'})
neighborhood_rank['price_rank'] = neighborhood_rank['price'].rank(ascending=False)
neighborhood_rank['total_listings_rank'] = neighborhood_rank['total_listings'].rank(ascending=False)

print_grouped_data(neighborhood_rank,'Rank data:')

agg_data.to_csv('aggregated_airbnb_data.csv')






