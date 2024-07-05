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

# The model was already created

@task
def get_customer_rating_from_mongo():
    url = "mongodb+srv://dmUser:Password123@dm.t9kyfvt.mongodb.net/?appName=DM" # replace <<CLUSTERNAME>> with your cluster's name and <<DATABASENAME>> with your database's name
    cluster = MongoClient(url)

    db = cluster["dm"] # replace <<DATABASENAME>> with your database's name
    collection = db["Merged_rating"]

    df = pd.DataFrame(list(collection.find()))
    return df



@task 
def segment_customers(customer_ratings):

    # Handle missing values
    kmeans_data = customer_ratings.dropna()

    # Load the model
    kmeans = joblib.load('customerSegmentation_up.joblib')

    kmeans_data = customer_ratings[['Monthly Revenue','Country','Age','Gender','movieId','rating', 'budget','popularity', 'revenue','runtime',  'vote_average','vote_count','_original_language']]
    # One-hot encode categorical features

    label_encoder = LabelEncoder()

    kmeans_data['Country'] = label_encoder.fit_transform(kmeans_data['Country'])
    kmeans_data['Gender'] = kmeans_data['Gender'].apply(lambda x: 0 if x.lower() == 'male' else 1 if x.lower() == 'female' else -1)

    # Handle missing values
    kmeans_data = kmeans_data.dropna()

    # Use the loaded model to make predictions
    customer_ratings['Cluster'] = kmeans.predict(kmeans_data)

    return customer_ratings



@task
def update_collection_cluster(new_data):
    url = "mongodb+srv://dmUser:Password123@dm.t9kyfvt.mongodb.net/?appName=DM" # replace <<CLUSTERNAME>> with your cluster's name and <<DATABASENAME>> with your database's name
    cluster = MongoClient(url)

    db = cluster["dm"] # replace <<DATABASENAME>> with your database's name
    collection = db["Merged_rating"]

    # Iterate over the merged DataFrame rows and update documents in MongoDB
    for index, row in new_data.iterrows():
        query = {'userId': row['userId'], 'movieId': row['movieId']}
        new_values = {'$set': {'Cluster': row['Cluster']}}
        collection.update_many(query, new_values)


@flow(log_prints=True)
def customer_segmentation():
    customer_ratings = get_customer_rating_from_mongo()
    customer_ratings = segment_customers(customer_ratings)
    if len(customer_ratings)!=0:
        update_collection_cluster(customer_ratings)

    



if __name__ == "__main__": 
    customer_segmentation.serve(name="Data Mining - Customer",
        tags=["dm", "customer segmentation"])
    




