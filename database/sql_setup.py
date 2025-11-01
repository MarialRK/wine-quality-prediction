import sqlite3
import pandas as pd
from datetime import datetime

def create_database():
    # Connect to SQLite database (creates if not exists)
    conn = sqlite3.connect('wine_quality.db')
    cursor = conn.cursor()
    
    # Table 1: wines - Main wine information
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS wines (
            wine_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            region TEXT,
            vintage INTEGER,
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Table 2: chemical_properties - Detailed chemical analysis
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS chemical_properties (
            analysis_id INTEGER PRIMARY KEY AUTOINCREMENT,
            wine_id INTEGER,
            fixed_acidity REAL,
            volatile_acidity REAL,
            citric_acid REAL,
            residual_sugar REAL,
            chlorides REAL,
            free_sulfur_dioxide REAL,
            total_sulfur_dioxide REAL,
            density REAL,
            ph REAL,
            sulphates REAL,
            alcohol REAL,
            FOREIGN KEY (wine_id) REFERENCES wines (wine_id)
        )
    ''')
    
    # Table 3: quality_ratings - Quality scores
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS quality_ratings (
            rating_id INTEGER PRIMARY KEY AUTOINCREMENT,
            wine_id INTEGER,
            quality_score INTEGER,
            quality_category TEXT,
            rated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (wine_id) REFERENCES wines (wine_id)
        )
    ''')
    
    # Table 4: wine_predictions - For ML predictions (Task 3)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS wine_predictions (
            prediction_id INTEGER PRIMARY KEY AUTOINCREMENT,
            wine_id INTEGER,
            predicted_quality INTEGER,
            confidence REAL,
            prediction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (wine_id) REFERENCES wines (wine_id)
        )
    ''')
    
    # STORED PROCEDURE: Insert wine with chemical properties
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS wine_log (
            log_id INTEGER PRIMARY KEY AUTOINCREMENT,
            wine_id INTEGER,
            action TEXT,
            log_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    print("Database schema created successfully!")
    
    # Load sample data from CSV
    load_sample_data(cursor)
    
    conn.commit()
    conn.close()

def load_sample_data(cursor):
    # Read the wine quality dataset
    df = pd.read_csv('data/wine_quality.csv', delimiter=';')
    
    # Insert sample data
    for i, row in df.head(10).iterrows():  # Load first 10 records as sample
        # Insert into wines table
        cursor.execute('''
            INSERT INTO wines (name, region, vintage) 
            VALUES (?, ?, ?)
        ''', (f"Wine_{i+1}", "Unknown", 2020))
        
        wine_id = cursor.lastrowid
        
        # Insert into chemical_properties table
        cursor.execute('''
            INSERT INTO chemical_properties 
            (wine_id, fixed_acidity, volatile_acidity, citric_acid, residual_sugar,
             chlorides, free_sulfur_dioxide, total_sulfur_dioxide, density, ph, sulphates, alcohol)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (wine_id, row['fixed acidity'], row['volatile acidity'], row['citric acid'],
              row['residual sugar'], row['chlorides'], row['free sulfur dioxide'],
              row['total sulfur dioxide'], row['density'], row['pH'], row['sulphates'],
              row['alcohol']))
        
        # Insert into quality_ratings table
        quality_category = "Good" if row['quality'] >= 6 else "Average"
        cursor.execute('''
            INSERT INTO quality_ratings (wine_id, quality_score, quality_category)
            VALUES (?, ?, ?)
        ''', (wine_id, int(row['quality']), quality_category))
    
    print("Sample data loaded successfully!")

if __name__ == "__main__":
    create_database()
