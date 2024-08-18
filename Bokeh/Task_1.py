# -*- coding: utf-8 -*-
"""
Created on Sun Aug 18 17:48:57 2024

@author: Влад
"""

from bokeh.plotting import figure, output_file, show,save
from bokeh.models import ColumnDataSource, FactorRange,HoverTool,MultiChoice
from bokeh.transform import factor_cmap, factor_mark
from bokeh.io import curdoc
from bokeh.layouts import column, row
import pandas as pd
import numpy as np
import os

def age_group_survival(data):
    # Age Group Survival: Create a bar chart showing survival rates across different age 
    # groups.

    df = data.groupby('AgeGroup')['Survived'].mean()*100
    df = df.reset_index()
    df['AgeGroup'] = df['AgeGroup'].astype(str)
    source = ColumnDataSource(df)
    p = figure(x_range=df['AgeGroup'].astype(str),title='Age Group Survival',x_axis_label='Age Group', y_axis_label='Survival Rate')
    p.vbar(x='AgeGroup', top='Survived', width=0.5, color='blue',source = source)
    hover = HoverTool(tooltips=[("Survived rate","@Survived"),
                                    ("Age Group","@AgeGroup")]) 
    p.add_tools(hover) 
    return p
    
def class_and_gender(data):
    # Class and Gender: Create a grouped bar chart to compare survival rates across 
    # different classes (1st, 2nd, 3rd) and genders (male, female)
    data['Pclass'] = data['Pclass'].astype(str)
    df = data.groupby(['Pclass','Sex'])['Survived'].mean() * 100 
    x = df.index.values
    counts = df.values
    source = ColumnDataSource(data=dict(x=x, counts=counts))  

    p = figure(x_range=FactorRange(*x), title="Class and Gender",
           toolbar_location=None, tools="")
    p.vbar(x='x', top='counts', width=0.9, source=source)
    p.yaxis.axis_label = 'Survival Rate'
    p.xaxis.axis_label = 'Gender, Class'
    hover = HoverTool(tooltips=[("Survived rate","@counts"),
                                ("Class,Gender","@x")]) 
    p.add_tools(hover) 
    return p
    
def fare_vs_survival(data):
    # Fare vs. Survival: Create a scatter plot with Fare on the x-axis and survival status 
    # on the y-axis, using different colors to represent different classes
    source = ColumnDataSource(data=data)
    p = figure(title = "Fare vs. Survival", background_fill_color="#fafafa")
    p.xaxis.axis_label = 'Petal Length'
    p.yaxis.axis_label = 'Sepal Width'
    SPECIES = sorted(data.Pclass.unique())
    MARKERS = ['hex', 'circle_x', 'triangle']
    p.scatter("Fare", "Survived", source=source, legend_group = 'Pclass',
          fill_alpha=0.4, size=12,
          marker=factor_mark('Pclass', MARKERS, SPECIES),
          color=factor_cmap('Pclass', 'Category10_3', SPECIES))
    hover = HoverTool(tooltips=[("Pclass","@Pclass"),
                                ("Fare","@Fare")]) 
    p.add_tools(hover) 
    p.xaxis.axis_label = 'Fare'
    p.yaxis.axis_label = 'Survived'
    p.legend.title = 'Class' 
    return p

os.chdir(os.path.dirname(__file__))
data = pd.read_csv('Titanic-Dataset.csv')

data['Age'].fillna(data['Age'].mean(), inplace=True)
data['Cabin'].fillna('Unknown', inplace=True)
data['Embarked'].fillna('Unknown', inplace=True)

data['AgeGroup'] = pd.cut(data['Age'],bins=[0,13,25,60,np.Inf],
                          labels = ['Child', 'Young Adult', 'Adult', 'Senior'],
                          ordered=False)

output_file("age_group_survival.html")
save(age_group_survival(data))

output_file("class_gender_survival.html")
save(class_and_gender(data))

output_file("fare_survival.html")
save(fare_vs_survival(data))


