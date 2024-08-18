# -*- coding: utf-8 -*-
"""
Created on Sat Aug 17 15:48:12 2024

@author: Vladyslav Hnatiuk
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import numpy as np
import os 

base_colors = mcolors.BASE_COLORS
color_names = list(base_colors.keys())


# 1. Neighborhood Distribution of Listing
def neighborhood_distribution_of_listings(data):
    neighborhoods = data['neighbourhood_group'].unique()
    colors = ['r','g','b','y','c']
    plt.figure(figsize=(12, 6))
    plt.bar(neighborhoods,
       data['neighbourhood_group'].value_counts(),
       color = colors)
    plt.title('Neighborhood Distribution of Listings')
    plt.xlabel('Neighborhood Group')
    plt.ylabel('Number of Listings')
    plt.savefig('neighborhood_distribution.png')
    plt.close()
    
# 2. Price Distribution Across Neighborhoods
def  price_distribution_across_neighborhoods(data):
    neighborhoods = data['neighbourhood_group'].unique()
    df = [data[data['neighbourhood_group'] == n]['price'] for n in neighborhoods]
    plt.figure(figsize=(12, 6))
    plt.boxplot(df,patch_artist=True, notch=True)
    plt.xticks(range(1, len(neighborhoods) + 1), neighborhoods)
    plt.title('Price Distribution Across Neighborhood')
    plt.xlabel('Neighborhood Group')
    plt.ylabel('Price')
    plt.savefig('price_distribution.png')
    plt.close()

# 3. Room Type vs. Availability
def room_type_vs_availability(data):
    df_mean = data.groupby(['neighbourhood_group', 'room_type'])['availability_365'].mean().unstack()
    df_sd = data.groupby(['neighbourhood_group', 'room_type'])['availability_365'].mean().unstack()
    df_mean.plot(kind='bar', yerr=df_sd, capsize=3, figsize=(12, 6),color=['r', 'g', 'b'])
    plt.title('Room Type vs. Availability')
    plt.xlabel('Neighborhood Group')
    plt.ylabel('Average Availability')
    plt.legend(title='Room Type')
    plt.savefig('room_type_availability.png')
    plt.close()

# 4. Correlation Between Price and Number of Review
def correlation_between_price_and_number_of_review(data):
    room_type_styles = {
        'Entire home/apt': ['o','red'],
        'Private room': ['^','green'],
        'Shared room': ['v','blue']
        }

    plt.figure(figsize=(12, 6))
    room_type = data['room_type'].unique()
    legend = []
    for rt,style in room_type_styles.items():
        df = data[data['room_type'] == rt]
        m, b = np.polyfit(df['price'], df['number_of_reviews'], 1)
        plt.scatter(x = df['price'],y = df['number_of_reviews'],
                        alpha=0.3,marker=style[0],c =style[1])
        legend.append(rt)
        plt.plot(df['price'],m*df['price']+b, color=style[1])
        legend.append(rt + '(Corelation line)')    
    plt.title('Correlation Between Price and Number of Reviews')
    plt.xlabel('Price')
    plt.ylabel('Number of Reviews') 
    plt.legend(legend,title='Room Type')
    plt.savefig('correlation_between_price_and_number_of_review.png')
    plt.close()


# 5. Time Series Analysis of Reviews
def time_series_analysis_of_reviews(data):
    #data['datetime'] = df.index.to_period('M')
    pt = data.pivot_table(index = 'last_review', columns = 'neighbourhood_group',values = 'number_of_reviews',aggfunc = 'mean')
    pt.fillna(method = 'ffill',inplace = True)
    pt.rolling(window=60).mean().plot(figsize=(12, 6))
    plt.title('Time Series Analysis of Reviews by Neighborhood Group')
    plt.xlabel('Time')
    plt.ylabel('Smoothed Average Number of Reviews')
    plt.legend(title='Neighborhood Group')
    plt.savefig('time_series_analysis_of_reviews.png')
    plt.close()
    
# 6. Price and Availability Heatmap
def price_and_availability_heatmap(data):
    data['price_category'] = pd.cut(data['price'], bins=[0, 100, 300, float('Inf')],
                                labels=['Low', 'Medium', 'High'],include_lowest=True)
    pivot = data.pivot_table(values='availability_365', index='neighbourhood_group', columns='price_category',
                                     aggfunc='mean')
    plt.imshow(pivot,cmap='viridis',aspect = 'auto') 
    plt.colorbar(label='Average Availability')
    plt.xticks(ticks=np.arange(len(pivot.columns)), labels=pivot.columns)
    plt.yticks(ticks=np.arange(len(pivot.index)), labels=pivot.index) 
    plt.title('Price and Availability Heatmap')
    plt.xlabel('Price')
    plt.ylabel('Neighborhood Group')
    plt.savefig('price_and_availability_heatmap.png')
    plt.close()

#7. Room Type and Review Count Analysis
def room_type_and_review_count_analysis(data):
    grouped = data.groupby(['neighbourhood_group', 'room_type'])['number_of_reviews'].sum().unstack()
    grouped.plot(kind='bar', stacked=True, figsize=(12, 6), color=['r', 'g', 'b'])
    plt.title('Review Count by Room Type Across Neighborhood Groups')
    plt.xlabel('Neighborhood Group')
    plt.ylabel('Total Number of Reviews')
    plt.legend(title='Room Type')
    plt.savefig('room_type_and_review_count_analysis.png')
    plt.close()

    
os.chdir(os.path.dirname(__file__))
data = pd.read_csv('AB_NYC_2019.csv')
    
neighborhood_distribution_of_listings(data)
price_distribution_across_neighborhoods(data)
room_type_vs_availability(data)
correlation_between_price_and_number_of_review(data)
time_series_analysis_of_reviews(data)
price_and_availability_heatmap(data)
room_type_and_review_count_analysis(data)