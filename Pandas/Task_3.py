# -*- coding: utf-8 -*-
"""
Created on Fri Aug 16 17:55:14 2024

@author: Vladyslav Hnatiuk
"""

import pandas as pd
import os 

def print_analysis_results(data, message=""):
    print(message)
    print(data)
    
def availability(x):
    # Create a new column availability_status using the apply function, 
    # classifying each listing into one of three categories based on the 
    # availability_365 column:
    #  ▪ "Rarely Available": Listings with fewer than 50 days of availability in 
    #    a year.
    #  ▪ "Occasionally Available": Listings with availability between 50 and 
    #    200 days.
    #  ▪ "Highly Available": Listings with more than 200 days of availability
    if x < 50:
        return 'Rarely Available'
    elif x <= 200:
        return 'Occasionally Available'
    else:
        return 'Highly Available' 

os.chdir(os.path.dirname(__file__))
data = pd.read_csv('cleaned_airbnb_data.csv')

# 1. Advanced Data Manipulation
# Analyze Pricing Trends Across Neighborhoods and Room Types
price_analyze = pd.pivot_table(data, values='price', index='neighbourhood_group', columns='room_type',
                                     aggfunc='mean')
print_analysis_results(price_analyze,'Pivot Table: ')
# Prepare Data for In-Depth Metric Analysis:
long_data = pd.melt(data, id_vars=['id', 'neighbourhood_group', 'room_type'],
                  value_vars=['price', 'minimum_nights'],
                      var_name='metric', value_name='value')

print_analysis_results(long_data,' Prepare Data for In-Depth Metric Analysis: ')

# Classify Listings by Availability:
data['availability_status'] = data['availability_365'].apply(availability)

# Analyze trends and patterns using the new availability_status column, and 
# investigate potential correlations between availability and other key 
# variables like price, number_of_reviews, and neighbourhood_group to 
# uncover insights that could inform marketing and operational strategies.
corr_analyze = data.groupby(['availability_status',
                             'neighbourhood_group'])[['number_of_reviews','price']].corr()

print_analysis_results(corr_analyze)

# 2. Descriptive Statistics

print_analysis_results(data[['price', 'minimum_nights', 'number_of_reviews']].describe(),
                       'Descriptive Statistics: ')

# 3. Time Series Analysis

# Convert the last_review column to a datetime object and set it as the index 
# of the DataFrame to facilitate time-based analyses

data['last_review'] = pd.to_datetime(data['last_review'])
data = data.set_index('last_review',drop = True)

# Identify Monthly Trends
# Resample the data to observe monthly trends in the number of reviews and 
# average prices, providing insights into how demand and pricing fluctuate 
# over time.
monthly_analysis = data.resample('M').agg({'number_of_reviews': 'sum','price': 'mean'}).dropna()

print_analysis_results(monthly_analysis, ' Identify Monthly Trends: ')

# Analyze Seasonal Patterns:
# Group the data by month to calculate monthly averages and analyze 
# seasonal patterns, enabling better forecasting and strategic planning around 
# peak periods.

data['month'] = data.index.month
seasonal_analysis = data.groupby('month').agg({'number_of_reviews': 'mean',}).reset_index()
print_analysis_results(seasonal_analysis, 'Analyze Seasonal Patterns: ')

seasonal_analysis.to_csv('time_series_airbnb_data.csv')








  

