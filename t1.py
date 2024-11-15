import sqlite3
import requests
import streamlit as st
import re
import os

# API Key and Database Path
GROQ_API_KEY = "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"
DB_PATH = "/Users/nileshhanotia/Projects/Firebase-ai-bot_1/valet_system copy.db"

# Function to preprocess SQL script for SQLite compatibility
def preprocess_sql_script(file_path):
    with open(file_path, 'r') as file:
        sql_script = file.read()

    # Remove unsupported SQL commands
    sql_script = re.sub(r"CREATE\s+DATABASE\s+\w+;", "", sql_script, flags=re.IGNORECASE)
    sql_script = re.sub(r"USE\s+\w+;", "", sql_script, flags=re.IGNORECASE)
    
    # Replace MySQL-specific keywords with SQLite-compatible keywords
    sql_script = sql_script.replace("AUTO_INCREMENT", "AUTOINCREMENT")
    sql_script = sql_script.replace("INT", "INTEGER")

    return sql_script

# Function to initialize the SQLite database from the preprocessed SQL file
def initialize_database(db_path):
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()

    # Preprocess SQL to remove unsupported commands
    sql_script = preprocess_sql_script(db_path)
    
    try:
        cursor.executescript(sql_script)
        conn.commit()
    except sqlite3.OperationalError as e:
        print(f"SQL Error: {e}")
    
    return conn

# Function to fetch the schema from the SQLite database
def fetch_schema(conn):
    schema = {}
    cursor = conn.cursor()

    # Query all tables from the sqlite_master table
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    for table in tables:
        table_name = table[0]
        schema[table_name] = {"columns": [], "relationships": []}

        # Get columns for each table
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = cursor.fetchall()

        for column in columns:
            column_name = column[1]
            column_type = column[2]
            schema[table_name]["columns"].append({"name": column_name, "type": column_type})

        # Get foreign key relationships for each table
        cursor.execute(f"PRAGMA foreign_key_list({table_name});")
        foreign_keys = cursor.fetchall()
        for fk in foreign_keys:
            fk_column = fk[3]
            referenced_table = fk[2]
            schema[table_name]["relationships"].append({"column": fk_column, "references": referenced_table})

    return schema

# Function to build SQL query based on schema and relationships
def generate_sql_from_schema(prompt, schema):
    tables_involved = []
    for table in schema:
        if table.lower() in prompt.lower():
            tables_involved.append(table)

    if not tables_involved:
        return "SELECT * FROM companies;"

    select_columns = []
    join_clauses = []

    for table in tables_involved:
        for column in schema[table]["columns"]:
            select_columns.append(f"{table}.{column['name']}")
        for relationship in schema[table]["relationships"]:
            referenced_table = relationship["references"]
            if referenced_table in tables_involved:
                join_clauses.append(f"JOIN {referenced_table} ON {table}.{relationship['column']} = {referenced_table}.{relationship['column']}")

    query = f"SELECT {', '.join(select_columns)} FROM {', '.join(tables_involved)}"
    if join_clauses:
        query += " " + " ".join(join_clauses)

    return query

# Function to interact with the GROQ API and generate SQL
def generate_sql_query(prompt):
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }

    messages = [
        {"role": "system", "content": "You are a helpful assistant that generates SQL queries from natural language."},
        {"role": "user", "content": prompt}
    ]
    payload = {
        "model": "llama3-8b-8192",
        "messages": messages,
        "temperature": 0.1,
        "max_tokens": 500
    }

    response = requests.post(url, headers=headers, json=payload)

    if response.status_code == 200:
        response_data = response.json()
        generated_query = response_data.get("choices", [{}])[0].get("message", {}).get("content", "").strip()
        return generated_query
    else:
        st.error(f"Error: {response.status_code}, {response.text}")
        return None

# Function to execute the generated SQL query on the local SQLite database
def execute_sql_query(query, conn):
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        return results

    except sqlite3.Error as e:
        st.error(f"SQLite Error: {e}")
        return None

# Streamlit UI
def main():
    conn = initialize_database(DB_PATH)
    schema = fetch_schema(conn)

    st.title("SQL Query Generator from Natural Language")

    prompt = st.text_area("Enter your query in natural language:", 
                          "I want to find all customers who purchased more than 10 items in the last 30 days, "
                          "along with the product names and quantities they bought. Also, include the customer's email and order date.")
    
    if st.button("Generate SQL Query"):
        if prompt.strip() != "":
            sql_query = generate_sql_from_schema(prompt, schema)

            if sql_query:
                st.subheader("Generated SQL Query:")
                st.code(sql_query)

                results = execute_sql_query(sql_query, conn)

                if results:
                    st.subheader("Query Results:")
                    st.write(results)
                else:
                    st.warning("No results found.")
            else:
                st.warning("Could not generate a valid SQL query.")
        else:
            st.warning("Please enter a query.")

if __name__ == "__main__":
    main()
