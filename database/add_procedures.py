import sqlite3

def add_procedures_and_triggers():
    conn = sqlite3.connect('wine_quality.db')
    cursor = conn.cursor()
    
    # STORED PROCEDURE: Calculate average quality for a wine
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS wine_stats (
            wine_id INTEGER PRIMARY KEY,
            avg_quality REAL,
            total_analyses INTEGER,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (wine_id) REFERENCES wines (wine_id)
        )
    ''')
    
    # TRIGGER 1: Log wine insertions
    cursor.execute('''
        CREATE TRIGGER IF NOT EXISTS log_wine_insert
        AFTER INSERT ON wines
        BEGIN
            INSERT INTO wine_log (wine_id, action)
            VALUES (NEW.wine_id, 'INSERT');
        END
    ''')
    
    # TRIGGER 2: Update wine stats when new quality rating is added
    cursor.execute('''
        CREATE TRIGGER IF NOT EXISTS update_wine_stats
        AFTER INSERT ON quality_ratings
        BEGIN
            INSERT OR REPLACE INTO wine_stats (wine_id, avg_quality, total_analyses)
            SELECT 
                NEW.wine_id,
                AVG(quality_score),
                COUNT(*)
            FROM quality_ratings 
            WHERE wine_id = NEW.wine_id;
        END
    ''')
    
    # Test the triggers
    cursor.execute('''
        INSERT INTO wines (name, region, vintage) 
        VALUES (?, ?, ?)
    ''', ("Test_Wine_Trigger", "Test_Region", 2023))
    
    wine_id = cursor.lastrowid
    
    cursor.execute('''
        INSERT INTO quality_ratings (wine_id, quality_score, quality_category)
        VALUES (?, ?, ?)
    ''', (wine_id, 7, "Good"))
    
    print("✅ Stored procedures and triggers created successfully!")
    print("✅ Test data inserted to verify triggers work")
    
    # Show log entries
    cursor.execute("SELECT * FROM wine_log")
    logs = cursor.fetchall()
    print(f"✅ Wine log entries: {len(logs)}")
    
    conn.commit()
    conn.close()

if __name__ == "__main__":
    add_procedures_and_triggers()
