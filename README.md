# -Census-Data-Standardization-and-Analysis-Pipeline
This project contains scripts for data cleaning, migration from MongoDB to MySQL, and a Streamlit application for data visualization.

## Table of Contents

1. [Introduction](#introduction)
2. [Installation](#installation)
3. [Usage](#usage)
4. [Scripts](#scripts)
   - [data_cleaning.py](#data_cleaningpy)
   - [mongodb_to_mysql.py](#mongodb_to_mysqlpy)
   - [streamlit.py](#streamlitpy)
5. [Contact](#contact)

## Introduction

The task is to clean, process, and analyze census data from a given source, including data renaming, missing data handling, state/UT name standardization, 
new state/UT formation handling, data storage, database connection, and querying. The goal is to ensure uniformity, accuracy, and accessibility of the 
census data for further analysis and visualization.


## Installation

To set up this project, clone the repository and install the necessary dependencies:


# Clone the repository
git clone https://github.com/Bennyjoseph07/Census-Data-Standardization-and-Analysis-Pipeline.git

# Navigate to the project directory
cd Census-Data-Standardization-and-Analysis-Pipeline

# Install dependencies
pip install -r requirements.txt

## Usage

data_cleaning.py
python data_cleaning.py

mongodb_to_mysql.py
python mongodb_to_mysql.py

streamlit.py
streamlit run streamlit.py

## Scripts
data_cleaning.py
This script is responsible for cleaning and preprocessing raw data. It includes functions to handle missing values, normalize data, and other preprocessing tasks.
And save the proccessed data to mongodb

mongodb_to_mysql.py
This script handles the migration of data from MongoDB to MySQL. It connects to both databases, fetches data from MongoDB, and inserts it into MySQL tables.

streamlit.py
This Streamlit application provides a user-friendly interface for visualizing the processed data. It includes various interactive components to explore the data.


## Contact
Created by Your Name - Benny Joseph
Mail - benny07joseph@gmail.com

Project Link: https://github.com/Bennyjoseph07/Census-Data-Standardization-and-Analysis-Pipeline
