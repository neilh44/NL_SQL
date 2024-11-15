import streamlit as st
import json
import os
import logging
from dotenv import load_dotenv
from LLM_Service import LLMService
import pandas as pd

# Initialize logging
logging.basicConfig(
    filename='app.log',  # Log file name
    level=logging.DEBUG,  # Set log level to DEBUG to capture all messages
    format='%(asctime)s - %(levelname)s - %(message)s'  # Log format
)

def main():
    st.title("Natural Language to JSON Query Generator")
    st.write("Enter your question about the JSON data in natural language.")
    
    # Load environment variables
    load_dotenv()
    
    # Define the path to the JSON folder
    json_folder_path = "/Users/nileshhanotia/Projects/Firebase-ai-bot_1/MJS/JSON"
    
    # Initialize LLM Service
    try:
        llm_service = LLMService(
            api_key=os.getenv("GROQ_API_KEY"),
            json_folder_path=json_folder_path
        )
        logging.info("LLMService initialized successfully.")
    except Exception as e:
        logging.error(f"Error initializing LLMService: {str(e)}")
        st.error(f"Error initializing service: {str(e)}")
        return

    # Input for natural language query
    natural_query = st.text_area(
        "Enter your query",
        "Show me all users from company 'XYZ'",
        height=100
    )

    if st.button("Generate and Execute Query"):
        if not natural_query:
            st.warning("Please enter a query.")
            logging.warning("User did not enter a query.")
            return

        # Convert to JSON query
        try:
            with st.spinner("Generating JSON query..."):
                json_result = llm_service.convert_to_json_query(natural_query)
            logging.info(f"Generated JSON query: {json_result['query']}")
        except Exception as e:
            logging.error(f"Error generating JSON query: {str(e)}")
            st.error(f"Error generating JSON query: {str(e)}")
            return

        if not json_result["success"]:
            logging.error(f"Error generating JSON query: {json_result['error']}")
            st.error(f"Error generating JSON query: {json_result['error']}")
            return

        # Display generated JSON query
        st.subheader("Generated JSON Query:")
        st.code(json_result["query"], language="json")

        # Execute query
        try:
            with st.spinner("Executing query on JSON data..."):
                query_result = llm_service.execute_json_query(json_result["query"])
            logging.info(f"Executed query successfully: {query_result['results']}")
        except Exception as e:
            logging.error(f"Error executing query on JSON data: {str(e)}")
            st.error(f"Error executing query: {str(e)}")
            return

        if not query_result["success"]:
            logging.error(f"Error executing query: {query_result['error']}")
            st.error(f"Error executing query: {query_result['error']}")
            return

        # Display results
        st.subheader("Query Results:")
        if query_result["results"]:
            df = pd.DataFrame(query_result["results"])
            st.dataframe(df)
        else:
            st.info("No results found.")
            logging.info("No results found for the query.")

if __name__ == "__main__":
    main()
