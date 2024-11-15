import streamlit as st
import pandas as pd
import os
from dotenv import load_dotenv
from llm_service import LLMService
from nl_converter import NLConverter
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    st.title("Natural Language to SQL Query Converter")
    st.write("Enter your question about the database in natural language.")
    
    # Load environment variables
    load_dotenv()
    
    api_key = os.getenv("GROQ_API_KEY")
    db_path = os.getenv("DB_PATH")
    
    if not api_key or not db_path:
        st.error("Missing API key or database path in environment variables.")
        logger.error("API key or DB path not found in environment variables.")
        return
    
    # Initialize LLM and NLConverter Services
    try:
        llm_service = LLMService(api_key=api_key, db_path=db_path)
        nl_converter = NLConverter(api_key=api_key)
    except Exception as e:
        st.error(f"Error initializing service: {str(e)}")
        return

    # Input for natural language query
    natural_query = st.text_area("Enter your query", "Show me all albums by artist 'Queen'", height=100)

    if st.button("Generate and Execute Query"):
        if not natural_query.strip():
            st.warning("Please enter a valid query.")
            return

        # Convert to SQL
        with st.spinner("Generating SQL query..."):
            sql_result = llm_service.convert_to_sql_query(natural_query)

        if not sql_result["success"]:
            st.error(f"Error generating SQL query: {sql_result['error']}")
            return

        # Display generated SQL
        st.subheader("Generated SQL Query:")
        st.code(sql_result["query"], language="sql")

        # Execute query
        with st.spinner("Executing query..."):
            query_result = llm_service.execute_query(sql_result["query"])

        if not query_result["success"]:
            st.error(f"Error executing query: {query_result['error']}")
            return

        # Display results in DataFrame format
        st.subheader("Query Results:")
        if query_result["results"]:
            df = pd.DataFrame(query_result["results"], columns=query_result["columns"])
            st.dataframe(df)
        else:
            st.info("No results found.")

        # Convert to Natural Language Explanation using NER entities
        with st.spinner("Generating natural language explanation..."):
            nl_result = nl_converter.convert_to_natural_language(query_result, natural_query)

        # Display natural language explanation
        st.subheader("Natural Language Explanation:")
        if nl_result["success"]:
            logger.info(f"Natural language explanation: {nl_result['explanation']}")
            st.write(nl_result["explanation"])
        else:
            logger.error(f"Error generating explanation: {nl_result['error']}")
            st.error(f"Error generating explanation: {nl_result['error']}")

if __name__ == "__main__":
    main()
