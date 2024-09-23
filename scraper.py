import os
import random
import time
import re
import json
from datetime import datetime
from typing import List, Dict, Type
import requests
import urllib.parse

import pandas as pd
from bs4 import BeautifulSoup
from pydantic import BaseModel, Field, create_model
import html2text
import tiktoken

from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


from openai import OpenAI
import google.generativeai as genai
from groq import Groq


from assets import USER_AGENTS,PRICING,HEADLESS_OPTIONS,SYSTEM_MESSAGE,USER_MESSAGE,GROQ_LLAMA_MODEL_FULLNAME
load_dotenv()

# Set up the Chrome WebDriver options

model_id = "phi3.5:latest"
encoded_model_id = urllib.parse.quote(model_id, safe='')  # Encodes all special characters
# encoded_model_id = 'phi3.5%3Alatest'

url = f"http://localhost:11434/v1/models/{encoded_model_id}/completions"

def setup_selenium():
    options = Options()

    # Randomly select a user agent from the imported list
    user_agent = random.choice(USER_AGENTS)
    options.add_argument(f"user-agent={user_agent}")

    # Add other options
    for option in HEADLESS_OPTIONS:
        options.add_argument(option)

    # Specify the path to the ChromeDriver
    service = Service(r"C:\ScrapeMaster\chromedriver-win64\chromedriver.exe")  

    # Initialize the WebDriver
    driver = webdriver.Chrome(service=service, options=options)
    return driver

def click_accept_cookies(driver):
    """
    Tries to find and click on a cookie consent button. It looks for several common patterns.
    """
    try:
        # Wait for cookie popup to load
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//button | //a | //div"))
        )
        
        # Common text variations for cookie buttons
        accept_text_variations = [
            "accept", "agree", "allow", "consent", "continue", "ok", "I agree", "got it"
        ]
        
        # Iterate through different element types and common text variations
        for tag in ["button", "a", "div"]:
            for text in accept_text_variations:
                try:
                    # Create an XPath to find the button by text
                    element = driver.find_element(By.XPATH, f"//{tag}[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{text}')]")
                    if element:
                        element.click()
                        print(f"Clicked the '{text}' button.")
                        return
                except:
                    continue

        print("No 'Accept Cookies' button found.")
    
    except Exception as e:
        print(f"Error finding 'Accept Cookies' button: {e}")

def fetch_html_selenium(url):
    driver = setup_selenium()
    try:
        driver.get(url)
        
        # Add random delays to mimic human behavior
        time.sleep(1)  # Adjust this to simulate time for user to read or interact
        driver.maximize_window()
        

        # Try to find and click the 'Accept Cookies' button
        # click_accept_cookies(driver)

        # Add more realistic actions like scrolling
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)  # Simulate time taken to scroll and read
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1)
        html = driver.page_source
        return html
    finally:
        driver.quit()

def clean_html(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Remove headers and footers based on common HTML tags or classes
    for element in soup.find_all(['header', 'footer']):
        element.decompose()  # Remove these tags and their content

    return str(soup)


def html_to_markdown_with_readability(html_content):

    
    cleaned_html = clean_html(html_content)  
    
    # Convert to markdown
    markdown_converter = html2text.HTML2Text()
    markdown_converter.ignore_links = False
    markdown_content = markdown_converter.handle(cleaned_html)
    
    return markdown_content


    
def save_raw_data(raw_data, timestamp, output_folder='output'):
    # Ensure the output folder exists
    os.makedirs(output_folder, exist_ok=True)
    
    # Save the raw markdown data with timestamp in filename
    raw_output_path = os.path.join(output_folder, f'rawData_{timestamp}.md')
    with open(raw_output_path, 'w', encoding='utf-8') as f:
        f.write(raw_data)
    print(f"Raw data saved to {raw_output_path}")
    return raw_output_path


def remove_urls_from_file(file_path):
    # Regex pattern to find URLs
    url_pattern = r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'

    # Construct the new file name
    base, ext = os.path.splitext(file_path)
    new_file_path = f"{base}_cleaned{ext}"

    # Read the original markdown content
    with open(file_path, 'r', encoding='utf-8') as file:
        markdown_content = file.read()

    # Replace all found URLs with an empty string
    cleaned_content = re.sub(url_pattern, '', markdown_content)

    # Write the cleaned content to a new file
    with open(new_file_path, 'w', encoding='utf-8') as file:
        file.write(cleaned_content)
    print(f"Cleaned file saved as: {new_file_path}")
    return cleaned_content


def create_dynamic_listing_model(field_names: List[str]) -> Type[BaseModel]:
    """
    Dynamically creates a Pydantic model based on provided fields.
    field_name is a list of names of the fields to extract from the markdown.
    """
    # Create field definitions using aliases for Field parameters
    field_definitions = {field: (str, ...) for field in field_names}
    # Dynamically create the model with all field
    return create_model('DynamicListingModel', **field_definitions)


def create_listings_container_model(listing_model: Type[BaseModel]) -> Type[BaseModel]:
    """
    Create a container model that holds a list of the given listing model.
    """
    return create_model('DynamicListingsContainer', listings=(List[listing_model], ...))




def trim_to_token_limit(text, model, max_tokens=120000):
    encoder = tiktoken.encoding_for_model(model)
    tokens = encoder.encode(text)
    if len(tokens) > max_tokens:
        trimmed_text = encoder.decode(tokens[:max_tokens])
        return trimmed_text
    return text

def generate_system_message(listing_model: BaseModel) -> str:
    """
    Dynamically generate a system message based on the fields in the provided listing model.
    """
    # Use the model_json_schema() method to introspect the Pydantic model
    schema_info = listing_model.model_json_schema()

    # Extract field descriptions from the schema
    field_descriptions = []
    for field_name, field_info in schema_info["properties"].items():
        # Get the field type from the schema info
        field_type = field_info["type"]
        field_descriptions.append(f'"{field_name}": "{field_type}"')

    # Create the JSON schema structure for the listings
    schema_structure = ",\n".join(field_descriptions)

    # Generate the system message dynamically
    system_message = f"""
    You are an intelligent text extraction and conversion assistant. Your task is to extract structured information 
from the given text and convert it into a pure JSON format enclosed within a code block. The JSON should contain 
only the structured data extracted from the text, with no additional commentary, explanations, or extraneous information.

Please ensure the output strictly follows this JSON schema and is enclosed within triple backticks.


    {{
        "listings": [
            {{
                {schema_structure}
            }}
        ]
    }} """

    return system_message



def format_data(data, DynamicListingsContainer, DynamicListingModel, selected_model):
    token_counts = {}
    
    if selected_model == "phi3.5:latest":
        # Correct endpoint usage for Ollama's API
        prompt = f"{SYSTEM_MESSAGE}\n{USER_MESSAGE}\n{data}"

        # Define the payload as per Ollama's API specifications
        payload = {
            "model": selected_model,
            "prompt": prompt,
            "max_tokens": 1200,
            "temperature": 0.7
        }

        try:
            # Use the correct generate endpoint for Ollama
            url = "http://localhost:11434/api/generate"
            
            # Make HTTP POST request to the Ollama API
            response = requests.post(
                url,
                headers={"Content-Type": "application/json"},
                json=payload,
                stream=True  # Enable streaming
            )
            
            # Print the HTTP status code for debugging
            print(f"HTTP Status Code: {response.status_code}")
            
            response.raise_for_status()  # Raises HTTPError for bad responses
            
            # Process the streaming response
            response_content = ""
            for line in response.iter_lines():
                if line:
                    try:
                        json_line = json.loads(line)
                        if 'response' in json_line:
                            response_content += json_line['response']
                    except json.JSONDecodeError:
                        # If it's not JSON, assume it's plain text
                        response_content += line.decode('utf-8')
            
            if not response_content:
                raise ValueError("No valid response received from Ollama API.")

            # Print the raw response text
            print("Raw Response Content:")
            print(response_content)
            
            # Treat the response as plain text
            parsed_response = {"text": response_content}
            
            # Calculate token counts using tiktoken
            try:
                encoder = tiktoken.encoding_for_model('gpt-3.5-turbo')  # Use a compatible encoding
            except KeyError:
                encoder = tiktoken.get_encoding("cl100k_base")  # Fallback encoding
            input_token_count = len(encoder.encode(prompt))
            output_token_count = len(encoder.encode(response_content))
            token_counts = {
                "input_tokens": input_token_count,
                "output_tokens": output_token_count
            }

            return parsed_response, token_counts

        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
            print(f"Response content: {response.text}")  # Print the response body for debugging
            raise ValueError(f"Ollama API request failed: {http_err}")
        except requests.exceptions.RequestException as e:
            print(f"Error communicating with Ollama API: {e}")
            raise ValueError(f"Ollama API request failed: {e}")
        except Exception as e:
            print(f"Unexpected error: {e}")
            print(f"Response content: {response_content}")
            raise ValueError(f"Failed to process Ollama API response: {e}")
            

def save_formatted_data(formatted_data, timestamp, output_folder='output'):
    # Ensure the output folder exists
    os.makedirs(output_folder, exist_ok=True)
    
    # Handle different types of formatted_data
    if isinstance(formatted_data, str):
        try:
            # Try to parse as JSON
            formatted_data_dict = json.loads(formatted_data)
        except json.JSONDecodeError:
            # If it's not JSON, treat it as plain text
            formatted_data_dict = {"text": formatted_data}
    else:
        # Handle data from OpenAI or other sources
        formatted_data_dict = formatted_data.dict() if hasattr(formatted_data, 'dict') else formatted_data

    # Save the formatted data as JSON with timestamp in filename
    json_output_path = os.path.join(output_folder, f'sorted_data_{timestamp}.json')
    with open(json_output_path, 'w', encoding='utf-8') as f:
        json.dump(formatted_data_dict, f, indent=4)
    print(f"Formatted data saved to JSON at {json_output_path}")

    # Prepare data for DataFrame
    if isinstance(formatted_data_dict, dict):
        if len(formatted_data_dict) == 1 and "text" in formatted_data_dict:
            # Handle plain text case
            data_for_df = [{"text": formatted_data_dict["text"]}]
        else:
            # If the data is a dictionary containing lists, assume these lists are records
            data_for_df = next(iter(formatted_data_dict.values())) if len(formatted_data_dict) == 1 else formatted_data_dict
    elif isinstance(formatted_data_dict, list):
        data_for_df = formatted_data_dict
    else:
        raise ValueError(f"Formatted data is neither a dictionary nor a list. Type: {type(formatted_data_dict)}")

    # Create DataFrame
    try:
        # Print debug information
        print(f"Data type for DataFrame: {type(data_for_df)}")
        print(f"Data for DataFrame: {data_for_df}")

        if isinstance(data_for_df, dict):
            # If it's a dict, create a DataFrame with a single row
            df = pd.DataFrame([data_for_df])
        elif isinstance(data_for_df, list) and all(isinstance(item, dict) for item in data_for_df):
            # If it's a list of dicts, create DataFrame directly
            df = pd.DataFrame(data_for_df)
        else:
            # For other cases, try to create a single-column DataFrame
            df = pd.DataFrame({'data': [data_for_df]})

        print("DataFrame created successfully.")
        print(f"DataFrame shape: {df.shape}")
        print(f"DataFrame columns: {df.columns}")

        # Save the DataFrame to an Excel file
        excel_output_path = os.path.join(output_folder, f'sorted_data_{timestamp}.xlsx')
        df.to_excel(excel_output_path, index=False)
        print(f"Formatted data saved to Excel at {excel_output_path}")
        
        return df
    except Exception as e:
        print(f"Error creating DataFrame or saving Excel: {str(e)}")
        print(f"Data causing the error: {data_for_df}")
        return None
    
def calculate_price(token_counts, model):
    input_token_count = token_counts.get("input_tokens", 0)
    output_token_count = token_counts.get("output_tokens", 0)
    
    # Calculate the costs
    input_cost = input_token_count * PRICING[model]["input"]
    output_cost = output_token_count * PRICING[model]["output"]
    total_cost = input_cost + output_cost
    
    return input_token_count, output_token_count, total_cost


if __name__ == "__main__":
    url = 'https://webscraper.io/test-sites/e-commerce/static'
    fields = ['Name of item', 'Price']

    try:
        # Generate timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Scrape data
        raw_html = fetch_html_selenium(url)
    
        markdown = html_to_markdown_with_readability(raw_html)
        
        # Save raw data
        save_raw_data(markdown, timestamp)

        # Create the dynamic listing model
        DynamicListingModel = create_dynamic_listing_model(fields)

        # Create the container model that holds a list of the dynamic listing models
        DynamicListingsContainer = create_listings_container_model(DynamicListingModel)
        
        # Format data using Ollama Phi3.5 model
        formatted_data, token_counts = format_data(
            markdown,
            DynamicListingsContainer,
            DynamicListingModel,
            "phi3.5:latest"  # Use the exact model ID as listed
        )
        print(formatted_data)
        
        # Save formatted data
        save_formatted_data(formatted_data, timestamp)

        # Convert formatted_data back to text for token counting
        formatted_data_text = json.dumps(
            formatted_data.dict() if hasattr(formatted_data, 'dict') else formatted_data
        )
        
        # Automatically calculate the token usage and cost for all input and output
        input_tokens, output_tokens, total_cost = calculate_price(token_counts, "phi3.5:latest")
        print(f"Input token count: {input_tokens}")
        print(f"Output token count: {output_tokens}")
        print(f"Estimated total cost: ${total_cost:.4f}")

    except Exception as e:
        print(f"An error occurred: {e}")



