import pandas as pd
import pymongo
from pymongo import MongoClient
import seaborn as sns
import matplotlib.pyplot as plt
import ast
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.preprocessing import LabelEncoder
import joblib
from prefect import flow, task
from prefect.deployments import Deployment
from prefect.client.schemas.schedules import CronSchedule

@task 
def movie_targeted_segmentation(new_movie_list):
    # processed_segmented_customers = preprocess(segmented_customers)

    label_encoder = LabelEncoder()

    loaded_model = joblib.load('movie_targeted_segmentation.joblib')

    X = new_movie_list[['budget', 'genres', 'movieId', 'popularity','revenue', 'runtime', 'vote_average', 'vote_count','_original_language']]
    X = X.dropna()
    X['genres'] = label_encoder.fit_transform(X['genres'])

    # Use the loaded model to make predictions
    X['Cluster'] = loaded_model.predict(X)

    return X


@task 
def update_movies(new_data):
    url = "mongodb+srv://dmUser:Password123@dm.t9kyfvt.mongodb.net/?appName=DM" # replace <<CLUSTERNAME>> with your cluster's name and <<DATABASENAME>> with your database's name
    cluster = MongoClient(url)

    db = cluster["dm"] # replace <<DATABASENAME>> with your database's name
    collection = db["movie_metadata"]

    # Iterate over the merged DataFrame rows and update documents in MongoDB
    for index, row in new_data.iterrows():
        query = {'movieId': row['movieId']}
        new_values = {'$set': {'Cluster': row['Cluster']}}
        collection.update_many(query, new_values)


@flow(log_prints=True)
def recommend_movie_marketing():
    new_movie_list = pd.read_csv('./Original DS/Testing_movies.csv').head(20)
    movie_list_with_cluster = movie_targeted_segmentation(new_movie_list)
    update_movies(movie_list_with_cluster)
    delete_processed_rows()


@task
def delete_processed_rows():
    Testing = pd.read_csv('./DS/Testing_movies.csv')
    Testing = Testing.iloc[20:]
    Testing.to_csv('./DS/Testing_movies.csv', index=False)

if __name__ == "__main__": 
    recommend_movie_marketing.serve(name="Data Mining - Market",
        tags=["dm", "marketing movies"])
    


