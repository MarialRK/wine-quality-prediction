# ERD Diagram for Wine Quality Database
def generate_erd_description():
    erd = """
    WINE QUALITY DATABASE - ERD DIAGRAM
    
    ENTITIES:
    1. WINES (Primary Table)
       - wine_id (PK)
       - name, region, vintage
       - created_date
    
    2. CHEMICAL_PROPERTIES 
       - analysis_id (PK)
       - wine_id (FK to WINES)
       - 11 chemical properties
       - alcohol content
    
    3. QUALITY_RATINGS
       - rating_id (PK) 
       - wine_id (FK to WINES)
       - quality_score (3-8)
       - quality_category
       - rated_date
    
    4. WINE_PREDICTIONS
       - prediction_id (PK)
       - wine_id (FK to WINES)
       - predicted_quality
       - confidence
       - prediction_date
    
    RELATIONSHIPS:
    - WINES 1:N CHEMICAL_PROPERTIES
    - WINES 1:N QUALITY_RATINGS
    - WINES 1:N WINE_PREDICTIONS
    
    CONSTRAINTS:
    - Foreign keys with referential integrity
    - Triggers: log_wine_insert, update_wine_stats  
    - Stored procedure: wine_stats maintenance
    """
    
    with open('docs/erd_diagram.txt', 'w', encoding='utf-8') as f:
        f.write(erd)
    
    print("ERD diagram description created in docs/erd_diagram.txt")
    print("Use dbdiagram.io to create visual diagram from this description")

if __name__ == "__main__":
    generate_erd_description()
