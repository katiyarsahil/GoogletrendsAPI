#Import Relevant Libraries
import pandas as pd
from pytrends.request import TrendReq

# Login to Google. Only need to run this once, the rest of requests will use the same session.
pytrend = TrendReq()

# Create payload and capture API tokens. Only needed for interest_over_time(), interest_by_region() & related_queries()
keyword = 'smart city'
pytrend.build_payload(kw_list=[keyword], timeframe='today 12-m', geo='US', gprop='')


# Capture Interest Over Time
interest_over_time_df = pytrend.interest_over_time()

#adding Date and Keyword data into dataframe
interest_over_time_df['Date'] = interest_over_time_df.index
interest_over_time_df['Searched_Keyword'] = keyword

#reorganizing Dataframe and checking results
interest_over_time_df=interest_over_time_df[['Searched_Keyword','Date', keyword, 'isPartial']]
interest_over_time_df=interest_over_time_df.rename(columns={ interest_over_time_df.columns[2]: 'Interest' })
interest_over_time_df.head()

# Capturing Interest by Region
interest_by_region_df = pytrend.interest_by_region()

#adding Date, State and Keyword data into dataframe

interest_by_region_df['Searched_Keyword'] = keyword
interest_by_region_df['State'] = interest_by_region_df.index
interest_by_region_df=interest_by_region_df.rename(columns={ interest_by_region_df.columns[0]: 'Interest' })

#reorganizing Dataframe and checking results
interest_by_region_df=interest_by_region_df[['Searched_Keyword','Interest','State']]
print(interest_by_region_df.head())

# Capturing Related Queries, returns a dictionary of dataframes
related_queries_dict = pytrend.related_queries()
list_of_dics = [value for value in related_queries_dict.values()]
related_queries_dictionary= list_of_dics[0]

#Adding Columns to Dataframe and Reorganizing 
rising_queries= related_queries_dictionary['rising']
rising_queries['Searched_Keyword'] = keyword
rising_queries=rising_queries[['Searched_Keyword','query', 'value']]

# Capturing Top Queries
top_queries= related_queries_dictionary['top']

#Adding Columns to Dataframe and Reorganizing 
top_queries['Searched_Keyword'] = keyword
top_queries=top_queries[['Searched_Keyword','query', 'value']]

# Get Google Keyword Suggestions
suggestions_dict = pytrend.suggestions(keyword)

keyword_suggestions = pd.DataFrame.from_records(suggestions_dict)

keyword_suggestions['Searched_keyword']=keyword
keyword_suggestions =keyword_suggestions [['Searched_keyword', 'mid', 'title', 'type']]
# importing Relevant libraries for SQL push
import sqlalchemy
import pyodbc
import urllib
#%% Initiate SQL Connection
params = urllib.parse.quote_plus("DRIVER={SQL Server};server=[server address];database=[database];uid=[user id];pwd=[password]") 
# Creating SQL engine and creating connection with Database
engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params) 
engine.connect() 
# Pushing Datafraemes into SQL Tables
keyword_suggestions.to_sql(name='[Table name]',con=engine, index=False, if_exists='append')
interest_over_time_df.to_sql(name='[Table name]',con=engine, index=False, if_exists='append')
interest_by_region_df.to_sql(name='[Table name]',con=engine, index=False, if_exists='append')
rising_queries.to_sql(name='[Table name]',con=engine, index=False, if_exists='append')
top_queries.to_sql(name='SMC_GOOGLE_TOP_QUERIES_T',con=engine, index=False, if_exists='append')
