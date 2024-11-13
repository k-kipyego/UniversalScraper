# pagination_detector.py

import os
import json
import re
from typing import List, Dict, Tuple, Union
from pydantic import BaseModel, Field, ValidationError

import tiktoken
from dotenv import load_dotenv

from openai import OpenAI
import google.generativeai as genai
from groq import Groq

from assets import PROMPT_PAGINATION, PRICING, LLAMA_MODEL_FULLNAME, GROQ_LLAMA_MODEL_FULLNAME

load_dotenv()
import logging

class PaginationData(BaseModel):
    page_urls: List[str] = Field(default_factory=list, description="List of pagination URLs, including 'Next' button URL if present")

def calculate_pagination_price(token_counts: Dict[str, int], model: str) -> float:
    """
    Calculate the price for pagination based on token counts and the selected model.
    
    Args:
    token_counts (Dict[str, int]): A dictionary containing 'input_tokens' and 'output_tokens'.
    model (str): The name of the selected model.

    Returns:
    float: The total price for the pagination operation.
    """
    input_tokens = token_counts['input_tokens']
    output_tokens = token_counts['output_tokens']
    
    input_price = input_tokens * PRICING[model]['input']
    output_price = output_tokens * PRICING[model]['output']
    
    return input_price + output_price

def detect_pagination_elements(url: str, indications: str, selected_model: str, markdown_content: str) -> Tuple[Union[PaginationData, Dict, str], Dict, float]:
    try:
        # Extract base URL components
        from urllib.parse import urlparse, urljoin
        parsed_url = urlparse(url)
        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"

        # Enhance the prompt to handle relative URLs
        prompt_pagination = f"""
        Analyze the provided markdown content and extract pagination elements. The current page URL is: {url}

        Important URL handling rules:
        1. For relative URLs starting with '/' (e.g., '/page/2'), combine with base URL: {base_url}
        2. For relative URLs without '/' (e.g., 'page/2'), combine with current URL directory
        3. For URLs with query parameters, preserve the essential parameters
        4. Return complete, absolute URLs only
        5. Look for common pagination patterns:
           - Numbered pages
           - Next/Previous links
           - Load more buttons
           - Infinite scroll markers
        6. Validate all URLs match the domain pattern of the original URL

        Return only valid, complete URLs that maintain the same domain as the source.
        """

        if indications:
            prompt_pagination += f"\nAdditional instructions: {indications}"
            
        if selected_model in ["gpt-4o-mini", "gpt-4o-2024-08-06"]:
            # Use OpenAI API
            client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
            completion = client.beta.chat.completions.parse(
                model=selected_model,
                messages=[
                    {"role": "system", "content": prompt_pagination},
                    {"role": "user", "content": markdown_content},
                ],
                response_format=PaginationData
            )

            # Extract the parsed response
            parsed_response = completion.choices[0].message.parsed

            # Calculate tokens using tiktoken
            encoder = tiktoken.encoding_for_model(selected_model)
            input_token_count = len(encoder.encode(markdown_content))
            output_token_count = len(encoder.encode(json.dumps(parsed_response.dict())))
            token_counts = {
                "input_tokens": input_token_count,
                "output_tokens": output_token_count
            }

            # Calculate the price
            pagination_price = calculate_pagination_price(token_counts, selected_model)

            # Check for "load more" or infinite scrolling patterns
            if "load more" in markdown_content.lower() or "infinite scroll" in markdown_content.lower():
                # Handle load more or infinite scrolling logic
                additional_urls = extract_load_more_urls(markdown_content)
                parsed_response.page_urls.extend(additional_urls)

            return parsed_response, token_counts, pagination_price

        elif selected_model == "gemini-1.5-flash":
            # Use Google Gemini API
            genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))
            model = genai.GenerativeModel(
                'gemini-1.5-flash',
                generation_config={
                    "response_mime_type": "application/json",
                    "response_schema": PaginationData
                }
            )
            prompt = f"{prompt_pagination}\n{markdown_content}"
            # Count input tokens using Gemini's method
            input_tokens = model.count_tokens(prompt)
            completion = model.generate_content(prompt)
            # Extract token counts from usage_metadata
            usage_metadata = completion.usage_metadata
            token_counts = {
                "input_tokens": usage_metadata.prompt_token_count,
                "output_tokens": usage_metadata.candidates_token_count
            }
            # Get the result
            response_content = completion.text
            
            # Log the response content and its type
            logging.info(f"Gemini Flash response type: {type(response_content)}")
            logging.info(f"Gemini Flash response content: {response_content}")
            
            # Try to parse the response as JSON
            try:
                parsed_data = json.loads(response_content)
                if isinstance(parsed_data, dict) and 'page_urls' in parsed_data:
                    pagination_data = PaginationData(**parsed_data)
                else:
                    pagination_data = PaginationData(page_urls=[])
            except json.JSONDecodeError:
                logging.error("Failed to parse Gemini Flash response as JSON")
                pagination_data = PaginationData(page_urls=[])

            # Calculate the price
            pagination_price = calculate_pagination_price(token_counts, selected_model)

            return pagination_data, token_counts, pagination_price


        elif selected_model == "Groq Llama3.1 70b":
            # Use Groq client
            client = Groq(api_key=os.environ.get("GROQ_API_KEY"))
            response = client.chat.completions.create(
                model=GROQ_LLAMA_MODEL_FULLNAME,
                messages=[
                    {"role": "system", "content": prompt_pagination},
                    {"role": "user", "content": markdown_content},
                ],
            )
            response_content = response.choices[0].message.content.strip()
            # Try to parse the JSON
            try:
                pagination_data = json.loads(response_content)
            except json.JSONDecodeError:
                pagination_data = {"page_urls": []}
            # Token counts
            token_counts = {
                "input_tokens": response.usage.prompt_tokens,
                "output_tokens": response.usage.completion_tokens
            }
            # Calculate the price
            pagination_price = calculate_pagination_price(token_counts, selected_model)

            # Ensure the pagination_data is a dictionary
            if isinstance(pagination_data, PaginationData):
                pagination_data = pagination_data.dict()
            elif not isinstance(pagination_data, dict):
                pagination_data = {"page_urls": []}

            return pagination_data, token_counts, pagination_price

        else:
            raise ValueError(f"Unsupported model: {selected_model}")

    except Exception as e:
        logging.error(f"An error occurred in detect_pagination_elements: {e}")
        # Return default values if an error occurs
        return PaginationData(page_urls=[]), {"input_tokens": 0, "output_tokens": 0}, 0.0

def extract_load_more_urls(markdown_content: str) -> List[str]:
    """Extract URLs for 'load more' or infinite scrolling from the markdown content."""
    # Example regex pattern to find "load more" links
    load_more_pattern = r'href=["\'](.*?)["\'].*?>\s*load more\s*</a>'
    matches = re.findall(load_more_pattern, markdown_content, re.IGNORECASE)

    # Example logic for infinite scrolling detection
    # This could be more complex depending on the structure of the content
    infinite_scroll_pattern = r'data-url=["\'](.*?)["\']'
    infinite_scroll_matches = re.findall(infinite_scroll_pattern, markdown_content)

    # Combine both lists of URLs
    return list(set(matches + infinite_scroll_matches))  # Remove duplicates