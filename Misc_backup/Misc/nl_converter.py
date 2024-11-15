import logging
import requests
import json
from typing import Dict, Any, List
import spacy
from spacy.cli import download

class NLConverter:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.api_url = "https://api.groq.com/openai/v1/chat/completions"
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }

        # Attempt to load spaCy model, and install if missing
        try:
            self.nlp = spacy.load("en_core_web_sm")
        except OSError:
            logging.info("Model 'en_core_web_sm' not found. Downloading...")
            download("en_core_web_sm")
            self.nlp = spacy.load("en_core_web_sm")
        
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def extract_entities(self, text: str) -> List[Dict[str, Any]]:
        """Use spaCy to extract named entities from text."""
        doc = self.nlp(text)
        entities = [{"text": ent.text, "label": ent.label_} for ent in doc.ents]
        self.logger.info(f"Extracted entities: {entities}")
        return entities

    def convert_to_natural_language(self, query_result: Dict[str, Any], original_query: str) -> Dict[str, Any]:
        """Convert query results to natural language using LLaMA-3 and spaCy for NER."""
        if not query_result["success"]:
            self.logger.error("Query execution failed; no results to process.")
            return {"success": False, "error": "No results to process"}

        formatted_data = [dict(zip(query_result["columns"], row)) for row in query_result["results"]]
        formatted_text = "\n".join([str(row) for row in formatted_data])

        entities = self.extract_entities(formatted_text)

        system_prompt = (
            "You are a data interpreter that uses named entities to create a clear, natural language explanation. "
            "Your job is to make sense of the given entities, summarize key insights, and answer the original question."
        )
        
        user_prompt = f"Original question: {original_query}\nExtracted Entities: {json.dumps(entities, indent=2)}\nPlease provide a natural language summary."

        try:
            payload = {
                "model": "llama3-8b-8192",
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                "max_tokens": 500,
                "temperature": 0.3
            }

            response = requests.post(self.api_url, headers=self.headers, json=payload, timeout=30)
            response.raise_for_status()
            
            result = response.json()
            if 'choices' in result and result['choices']:
                explanation = result['choices'][0]['message']['content'].strip()
                self.logger.info(f"Generated natural language explanation: {explanation}")
                return {"success": True, "explanation": explanation}
            
            return {"success": False, "error": "Failed to generate explanation"}

        except requests.exceptions.RequestException as e:
            self.logger.error(f"API request error: {str(e)}")
            return {"success": False, "error": f"API request failed: {str(e)}"}
