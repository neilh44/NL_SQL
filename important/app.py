import streamlit as st
import pandas as pd
import os
from dotenv import load_dotenv
from llm_service import LLMService

def main():
    st.title("Natural Language to SQL Query Converter")
    st.write("Enter your question about the database in natural language.")
    
    # Load environment variables
    load_dotenv()
    
    # Initialize LLM Service
    try:
        llm_service = LLMService(
            api_key=os.getenv("GROQ_API_KEY"),
            db_path=os.getenv("DB_PATH")
        )
    except Exception as e:
        st.error(f"Error initializing service: {str(e)}")
        return

    # Input for natural language query
    natural_query = st.text_area(
        "Enter your query",
        "Show me all albums by artist 'Queen'",
        height=100
    )

    if st.button("Generate and Execute Query"):
        if not natural_query:
            st.warning("Please enter a query.")
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

        # Display results
        st.subheader("Query Results:")
        if query_result["results"]:
            df = pd.DataFrame(
                query_result["results"],
                columns=query_result["columns"]
            )
            st.dataframe(df)
        else:
            st.info("No results found.")

if __name__ == "__main__":
    main()
