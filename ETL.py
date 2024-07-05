import pandas as pd
import pymongo
from pymongo import MongoClient
import seaborn as sns
import matplotlib.pyplot as plt
import ast
from prefect import flow, task
from prefect.deployments import Deployment
from prefect.client.schemas.schedules import CronSchedule
from sklearn.preprocessing import LabelEncoder


@task
def extract():
    movie_metadata = pd.read_csv('./DS/movies_metadata.csv')
    userbase = pd.read_csv('./DS/Netflix Userbase.csv')
    ratings = pd.read_csv('./DS/ratings.csv').head(10)
    return movie_metadata, userbase, ratings


@task
def extract_id(value):
    if pd.isnull(value):
        return value
    
    try:
        dict_value = ast.literal_eval(value)
        return dict_value['id']
    except(ValueError, SyntaxError):
        return value

def extract_genres(genres_str):
    try:
        genres_list = ast.literal_eval(genres_str)
        return [genre['name'] for genre in genres_list]
    except (ValueError, SyntaxError):
        return []

@task
def one_hot_encode_genres(df, genres_col='genres'):
    # Extract all unique genres
    df['genres_list'] = df[genres_col].apply(extract_genres)
    all_genres = set(genre for genres in df['genres_list'] for genre in genres)
    
    # Create a column for each genre
    for genre in all_genres:
        df[genre] = df['genres_list'].apply(lambda x: 1 if genre in x else 0)
    
    # Drop the temporary genres_list column
    df.drop(columns=['genres_list'], inplace=True)
    
    return df

def extract_production_country(production_str):
    try:
        production_list = ast.literal_eval(production_str)
        return [production['name'] for production in production_list]
    except (ValueError, SyntaxError):
        return []

@task
def one_hot_encode_production_country(df, production_countries_col='production_countries'):
    # Extract all unique genres
    df['production_countries_list'] = df[production_countries_col].apply(extract_production_country)
    all_countries = set(country for countries in df['production_countries_list'] for country in countries)
    
    # Create a column for each genre
    for country in all_countries:
        df[country] = df['production_countries_list'].apply(lambda x: 1 if country in x else 0)
    
    # Drop the temporary genres_list column
    df.drop(columns=['production_countries_list'], inplace=True)
    
    return df
    
@task
def transform(movie_metadata, userbase, ratings):
    userbase.rename(columns={'User ID':'userId'}, inplace=True)
        
    userbase_ratings = pd.merge(userbase, ratings, on='userId', how='inner')
    merged_df = pd.merge(userbase_ratings, movie_metadata, on='movieId', how='inner')
    merged_df['belongs_to_collection'] = merged_df['belongs_to_collection'].apply(extract_id)

    merged_df = merged_df.drop(columns=["Subscription Type", 'Last Payment Date', 'Device','Plan Duration','timestamp'])
    return merged_df


@task
def preprocess_movies(movie):
    movie.rename(columns={'id':'movieId'}, inplace=True)
    movie['movieId_num'] = pd.to_numeric(movie['movieId'], errors= 'coerce')
    movie = movie.dropna(subset=['movieId_num'])
    movie['movieId']= movie['movieId'].astype(int)
    movie = movie.drop(columns=['movieId_num','homepage','imdb_id','poster_path','spoken_languages', 'status','video'], axis = 1)
    label_encoder = LabelEncoder()
    # Assuming your genres column has string representations of lists, convert them to actual lists

    movie['genres'] = movie['genres'].apply(lambda x: ast.literal_eval(x) if pd.notna(x) else [])
    movie['production_companies'] = movie['production_companies'].apply(lambda x: ast.literal_eval(x) if pd.notna(x) else [])

    # Function to convert the list of dictionaries to a comma-separated string of genres
    def list_to_string(list):
        return ', '.join([item['name'] for item in list])

    # Apply the function to update the genres column
    movie['genres'] = movie['genres'].apply(list_to_string)
    movie['production_companies'] = movie['production_companies'].apply(list_to_string)
    # movie  = one_hot_encode_genres(movie)
    # movie = one_hot_encode_production_country(movie)
    
    # Initialize LabelEncoder
    label_encoder = LabelEncoder()
    movie['_original_language'] = label_encoder.fit_transform(movie['original_language'])
    return movie


@task
def load(merged_df):
    url = "mongodb+srv://dmUser:Password123@dm.t9kyfvt.mongodb.net/?appName=DM" # replace <<CLUSTERNAME>> with your cluster's name and <<DATABASENAME>> with your database's name
    cluster = MongoClient(url)

    db = cluster["dm"] # replace <<DATABASENAME>> with your database's name
    collection = db["Merged_rating"]

    collection.insert_many(merged_df.to_dict(orient='records'))


@task
def delete_processed_rows(ratings):
    ratings = ratings.iloc[10:]
    ratings.to_csv('./DS/ratings.csv', index=False)



@flow(log_prints=True)
def main():
    movie_metadata, userbase, ratings = extract()
    movie_metadata = preprocess_movies(movie_metadata)
    merged_df = transform(movie_metadata, userbase, ratings)
    print("Merged Size is = " , merged_df.size)
    if len(merged_df)!=0:
        load(merged_df)
        delete_processed_rows(ratings)


if __name__ == "__main__":
    main.serve(name="ETL",
        tags=["dm", "etl"])
    

# # Define a schedule to run the flow every day at 12:00 AM
# schedule = CronSchedule(cron="0 0 * * *")

# deployment = Deployment.build_from_flow(
#     flow=main,
#     name="daily-scheduled-flow",
#     schedule=schedule
# )

# if __name__ == "__main__":
#     deployment.apply()
#     from prefect.cli import app
#     app()

#     # Run the Prefect agent
#     import os
#     os.system("prefect agent start")