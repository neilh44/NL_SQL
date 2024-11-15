import sqlite3
import pandas as pd

# Define the database path
db_path = '/Users/nileshhanotia/Projects/Firebase-ai-bot_1/valet_system.db'

# SQL query to execute
sql_query = """
SELECT locations.* 
FROM locations 
JOIN companies ON locations.company_id = companies.id 
WHERE companies.company_name = 'DevTest';
"""

# Connect to the SQLite database
try:
    connection = sqlite3.connect(db_path)
    cursor = connection.cursor()
    
    # Execute the SQL query
    cursor.execute(sql_query)
    
    # Fetch all results
    results = cursor.fetchall()
    
    # Fetch column names
    column_names = [description[0] for description in cursor.description]
    
    # Close the database connection
    connection.close()

    # If there are results, display them as a DataFrame
    if results:
        df = pd.DataFrame(results, columns=column_names)
        print(df)  # You can replace this with st.write(df) if using Streamlit
    else:
        print("No results found for the query.")
    
except sqlite3.Error as e:
    print(f"An error occurred: {e}")
