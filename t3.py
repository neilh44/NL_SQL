import sqlite3

# Database path
DB_PATH = "/Users/nileshhanotia/Projects/Firebase-ai-bot_1/Misc/sample.db"

# Function to fetch and print relationships between tables
def print_table_relationships(db_path):
    # Connect to the database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Fetch all table names from the database
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [table[0] for table in cursor.fetchall()]

    # Dictionary to store relationships
    relationships = {}

    for table in tables:
        relationships[table] = []
        # Fetch foreign key information for each table
        cursor.execute(f"PRAGMA foreign_key_list({table});")
        foreign_keys = cursor.fetchall()
        
        for fk in foreign_keys:
            referenced_table = fk[2]  # The referenced table name
            from_column = fk[3]       # Column in the current table
            to_column = fk[4]         # Column in the referenced table

            # Store relationship
            relationships[table].append({
                "from_column": from_column,
                "referenced_table": referenced_table,
                "to_column": to_column
            })

    conn.close()

    # Print relationships
    for table, relations in relationships.items():
        print(f"Table '{table}' relationships:")
        if relations:
            for relation in relations:
                print(f"  - {relation['from_column']} references {relation['referenced_table']}({relation['to_column']})")
        else:
            print("  - No foreign key relationships.")
        print()  # Newline for readability

# Execute the function
print_table_relationships(DB_PATH)
