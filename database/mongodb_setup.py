from pymongo import MongoClient
import pandas as pd
from datetime import datetime

def setup_mongodb():
    # MongoDB Atlas connection string
    connection_string = "mongodb+srv://dkudum_db_user:GPLusKiPgfcTJQBJ@cluster0.lpotyed.mongodb.net/?appName=Cluster0"
    
    try:
        # Connect to MongoDB
        client = MongoClient(connection_string)
        
        # Test connection
        client.admin.command('ping')
        print("✅ Successfully connected to MongoDB Atlas!")
        
        # Create database
        db = client['wine_quality_db']
        
        # Collection 1: wines
        wines_collection = db['wines']
        
        # Collection 2: chemical_analysis
        chemical_collection = db['chemical_analysis']
        
        # Collection 3: predictions
        predictions_collection = db['predictions']
        
        # Load sample data from CSV
        df = pd.read_csv('data/wine_quality.csv', delimiter=';')
        
        # Insert sample data into MongoDB
        for i, row in df.head(10).iterrows():
            # Insert wine document
            wine_doc = {
                "wine_id": i + 1,
                "name": f"Wine_{i+1}",
                "region": "Unknown",
                "vintage": 2020,
                "created_date": datetime.now()
            }
            wine_result = wines_collection.insert_one(wine_doc)
            
            # Insert chemical analysis document
            chemical_doc = {
                "wine_id": i + 1,
                "fixed_acidity": row['fixed acidity'],
                "volatile_acidity": row['volatile acidity'],
                "citric_acid": row['citric acid'],
                "residual_sugar": row['residual sugar'],
                "chlorides": row['chlorides'],
                "free_sulfur_dioxide": row['free sulfur dioxide'],
                "total_sulfur_dioxide": row['total sulfur dioxide'],
                "density": row['density'],
                "ph": row['pH'],
                "sulphates": row['sulphates'],
                "alcohol": row['alcohol'],
                "quality": int(row['quality']),
                "analysis_date": datetime.now()
            }
            chemical_collection.insert_one(chemical_doc)
        
        print(f"✅ Inserted {len(df.head(10))} sample records into MongoDB")
        print("✅ Collections created: wines, chemical_analysis, predictions")
        
        # Show collections
        print(f"✅ Database collections: {db.list_collection_names()}")
        
    except Exception as e:
        print(f"❌ Error connecting to MongoDB: {e}")

if __name__ == "__main__":
    setup_mongodb()
