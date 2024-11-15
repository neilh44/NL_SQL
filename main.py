import streamlit as st
from llm_service import LLMService
import sqlite3
import logging
import pandas as pd  # Import pandas for displaying results as a DataFrame

# Initialize logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('app.log')
    ]
)
logger = logging.getLogger(__name__)

# Define the API key and database path
api_key = "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"  # Replace with your actual Groq API key
db_path = "/Users/nileshhanotia/Projects/Firebase-ai-bot_1/New_valet_system.db"  # Replace with the path to your SQLite database

# Initialize LLMService
llm_service = LLMService(api_key, db_path)

# Streamlit UI
st.title("Natural Language Query System")
st.write("Enter your natural language query below to retrieve results")

# User input for natural language query
natural_query = st.text_input("Enter your query", "sw me locations of company name DevTest")

if st.button("Generate Answer"):
    try:
        logger.info(f"Received query: {natural_query}")

        # Ask Llama model to analyze the schema and verify which tables and columns are used
        # Create a prompt to send to Llama to verify which tables should be used
        schema_analysis_query = f"Analyze the following schema and generate an SQL query for the natural language query: '{natural_query}' based on the following relationships between the tables: \n\n"
        schema_analysis_query += """
        - 'companies' table has columns: id, company_name
        - 'locations' table has columns: id, company_id, location_name
        - 'users' table has columns: id, company_id, name, balance
        - 'tickets' table has columns: id, location_id, ticket_name
        - 'activities' table has columns: id, location_id, ticket_id, activity_name
        """

        # Ask Llama model to generate the SQL query and identify used tables
        sql_query = llm_service.convert_to_sql_query(schema_analysis_query)

        # Display the generated SQL query in the Streamlit app
        st.subheader("Generated SQL Query")
        st.write(sql_query)  # Print the SQL query in the app interface

        # Log the generated SQL query
        logger.info(f"Generated SQL query: {sql_query}")

        if "SELECT" in sql_query:
            # Execute the generated SQL query on the SQLite database
            connection = sqlite3.connect(db_path)
            cursor = connection.cursor()

            # Execute the query and fetch the results
            cursor.execute(sql_query)
            results = cursor.fetchall()

            # Fetch column names to display in DataFrame
            column_names = [description[0] for description in cursor.description]
            connection.close()

            # Display results in a structured format
            if results:
                st.subheader("Query Results")
                # Convert results to pandas DataFrame for structured display
                df = pd.DataFrame(results, columns=column_names)
                st.dataframe(df)  # Display the results in a table format
            else:
                st.write("No results found for the query.")
                logger.info("No results found for the query.")
        else:
            st.warning("The generated query is not a SELECT query, so no results can be fetched.")
            logger.warning("The generated query is not a SELECT query, so no results can be fetched.")

    except Exception as e:
        logger.error(f"Error generating or executing the SQL query: {str(e)}")
        st.error(f"Error: {str(e)}")
