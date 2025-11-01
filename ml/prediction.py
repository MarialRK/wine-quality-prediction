import requests
import json
from datetime import datetime

class WinePredictor:
    def __init__(self, base_url="http://localhost:8000"):
        self.base_url = base_url
    
    def fetch_latest_wine_data(self):
        """Fetch the latest wine data from the API"""
        try:
            response = requests.get(f"{self.base_url}/wines/")
            if response.status_code == 200:
                wines = response.json()
                if wines:
                    # Get the latest wine (highest ID)
                    latest_wine = max(wines, key=lambda x: x['wine_id'])
                    print(f"✅ Fetched latest wine: {latest_wine}")
                    return latest_wine
                else:
                    print("❌ No wines found in database")
                    return None
            else:
                print(f"❌ Error fetching wines: {response.status_code}")
                return None
        except Exception as e:
            print(f"❌ Error: {e}")
            return None
    
    def simulate_ml_prediction(self, wine_data):
        """Simulate ML prediction"""
        wine_id = wine_data['wine_id']
        
        # Simple simulation
        predicted_quality = (wine_id % 3) + 5
        confidence = 0.75 + (wine_id % 10) * 0.02
        
        print(f"🎯 Simulated prediction for wine {wine_id}:")
        print(f"   - Predicted Quality: {predicted_quality}")
        print(f"   - Confidence: {confidence:.2f}")
        
        return predicted_quality, confidence
    
    def save_prediction_to_db(self, wine_id, predicted_quality, confidence):
        """Save prediction to SQL database"""
        try:
            import sqlite3
            conn = sqlite3.connect('../wine_quality.db')
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO wine_predictions (wine_id, predicted_quality, confidence)
                VALUES (?, ?, ?)
            ''', (wine_id, predicted_quality, confidence))
            
            conn.commit()
            conn.close()
            print(f"✅ Prediction saved to SQL database for wine {wine_id}")
            return True
        except Exception as e:
            print(f"❌ Error saving prediction: {e}")
            return False
    
    def save_prediction_to_mongodb(self, wine_id, predicted_quality, confidence):
        """Save prediction to MongoDB"""
        try:
            from pymongo import MongoClient
            client = MongoClient("mongodb+srv://dkudum_db_user:GPLusKiPgfcTJQBJ@cluster0.lpotyed.mongodb.net/?appName=Cluster0")
            db = client['wine_quality_db']
            
            prediction_doc = {
                "wine_id": wine_id,
                "predicted_quality": predicted_quality,
                "confidence": confidence,
                "prediction_date": datetime.now(),
                "model_used": "simulated_quality_predictor"
            }
            
            result = db.predictions.insert_one(prediction_doc)
            print(f"✅ Prediction saved to MongoDB with ID: {result.inserted_id}")
            return True
        except Exception as e:
            print(f"❌ Error saving to MongoDB: {e}")
            return False
    
    def run_prediction_pipeline(self):
        """Run the complete prediction pipeline"""
        print("🚀 Starting Wine Quality Prediction Pipeline...")
        
        # Step 1: Fetch latest data
        print("\n📥 Step 1: Fetching latest wine data...")
        latest_wine = self.fetch_latest_wine_data()
        
        if not latest_wine:
            print("❌ Cannot proceed without wine data")
            return
        
        # Step 2: Make prediction
        print("\n🤖 Step 2: Making quality prediction...")
        predicted_quality, confidence = self.simulate_ml_prediction(latest_wine)
        
        # Step 3: Save to databases
        print("\n💾 Step 3: Saving predictions to databases...")
        sql_success = self.save_prediction_to_db(latest_wine['wine_id'], predicted_quality, confidence)
        mongo_success = self.save_prediction_to_mongodb(latest_wine['wine_id'], predicted_quality, confidence)
        
        if sql_success and mongo_success:
            print("\n🎉 Prediction pipeline completed successfully!")
        else:
            print("\n⚠️  Pipeline completed with some warnings")

if __name__ == "__main__":
    # Make sure FastAPI server is running first!
    predictor = WinePredictor()
    predictor.run_prediction_pipeline()
