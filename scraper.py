import os
import random
import time
import re
import json
import logging
from datetime import datetime
from typing import List, Dict, Type

import pandas as pd
from bs4 import BeautifulSoup
from pydantic import BaseModel, Field, create_model
import html2text
import requests
import tiktoken

from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementClickInterceptedException, StaleElementReferenceException

from openai import OpenAI
import google.generativeai as genai
from groq import Groq


from assets import USER_AGENTS,PRICING,HEADLESS_OPTIONS,SYSTEM_MESSAGE,USER_MESSAGE, GROQ_LLAMA_MODEL_FULLNAME, TIMEOUT_SETTINGS
from groq import Groq

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scraper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()
# Set up the Chrome WebDriver options

def setup_selenium():
    options = Options()

    # Randomly select a user agent from the imported list
    user_agent = random.choice(USER_AGENTS)
    options.add_argument(f"user-agent={user_agent}")

    # Add other options
    for option in HEADLESS_OPTIONS:
        options.add_argument(option)

    # Specify the path to the ChromeDriver
    service = Service("C:/ScrapeMaster/chromedriver-win64/chromedriver.exe")  

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

def fetch_html_selenium(url, max_pages=10):
    driver = setup_selenium()
    try:
        driver.get(url)
        time.sleep(random.uniform(1, 3))
        driver.maximize_window()
        click_accept_cookies(driver)
        
        # Use the updated handle_pagination function
        list_of_html_pages = handle_pagination(driver, max_pages=max_pages)
        
        return list_of_html_pages
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
                        from the given text and convert it into a pure JSON format. The JSON should contain only the structured data extracted from the text, 
                        with no additional commentary, explanations, or extraneous information. 
                        You could encounter cases where you can't find the data of the fields you have to extract or the data will be in a foreign language.
                        Please process the following text and provide the output in pure JSON format with no words before or after the JSON:
    Please ensure the output strictly follows this schema:

    {{
        "listings": [
            {{
                {schema_structure}
            }}
        ]
    }} """
    system_message += "\nThe input will be provided as markdown text enclosed in triple backticks. Extract the required information from this markdown."
    return system_message



def format_data(data, DynamicListingsContainer, DynamicListingModel, selected_model):
    token_counts = {}

    # Convert data to string if it's a list
    if isinstance(data, list):
        data = json.dumps(data)  # Convert list to JSON string

    if selected_model in ["gpt-4o-mini", "gpt-4o-2024-08-06"]:
        # Use OpenAI API
        client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
        try:
            completion = client.beta.chat.completions.parse(
                model=selected_model,
                messages=[
                    {"role": "system", "content": SYSTEM_MESSAGE},
                    {"role": "user", "content": USER_MESSAGE + data},
                ],
                response_format=DynamicListingsContainer
            )
            # Calculate tokens using tiktoken
            encoder = tiktoken.encoding_for_model(selected_model)
            input_token_count = len(encoder.encode(USER_MESSAGE + data))
            output_token_count = len(encoder.encode(json.dumps(completion.choices[0].message.parsed.dict())))
            token_counts = {
                "input_tokens": input_token_count,
                "output_tokens": output_token_count
            }
            logger.info(f"Successfully processed data with {selected_model}")
            return completion.choices[0].message.parsed, token_counts
        except Exception as e:
            logger.error(f"Error occurred while calling OpenAI API: {e}")
            raise ValueError(f"Error in OpenAI API call: {e}")

    elif selected_model == "Groq Llama3.1 70b":
        # Dynamically generate the system message based on the schema
        sys_message = generate_system_message(DynamicListingModel)
        logger.debug(f"Generated system message: {sys_message}")

        # Point to the local server
        client = Groq(api_key=os.environ.get("GROQ_API_KEY"))

        try:
            # Ensure data is a string for Groq API
            if isinstance(data, list):
                data = json.dumps(data)  # Convert list to JSON string

            completion = client.chat.completions.create(
                messages=[
                    {"role": "system", "content": sys_message},
                    {"role": "user", "content": USER_MESSAGE + data}
                ],
                model=GROQ_LLAMA_MODEL_FULLNAME,
            )

            # Extract the content from the response
            response_content = completion.choices[0].message.content
            
            # Add debug logging
            logger.debug(f"Raw response from Groq API: {response_content}")
            
            # Check if the response is empty or not in the expected format
            if not response_content.strip():
                logger.error("Empty response received from Groq API")
                raise ValueError("Empty response received from Groq API")
            
            # Attempt to parse the JSON response
            try:
                parsed_response = json.loads(response_content)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse JSON response: {e}")
                logger.error(f"Raw response causing the error: {response_content}")
                
                # Attempt to clean and fix the JSON
                cleaned_response = response_content.strip()
                if not cleaned_response.startswith('{'):
                    cleaned_response = '{' + cleaned_response
                if not cleaned_response.endswith('}'):
                    cleaned_response += '}'
                
                try:
                    parsed_response = json.loads(cleaned_response)
                    logger.info("Successfully parsed JSON after cleaning")
                except json.JSONDecodeError:
                    logger.error("Failed to parse JSON even after cleaning")
                    raise ValueError(f"Invalid JSON response from Groq API: {e}")
            
            # Token counts
            token_counts = {
                "input_tokens": completion.usage.prompt_tokens,
                "output_tokens": completion.usage.completion_tokens
            }

            return parsed_response, token_counts

        except Exception as e:
            logger.exception(f"Error occurred while calling Groq API: {e}")
            raise ValueError(f"Error in Groq API call: {e}")

    else:
        raise ValueError(f"Unsupported model: {selected_model}")

def find_next_element(driver):
    """
    Attempts to find the 'Next' button or link using various selectors.
    """
    # Dismiss cookie consent overlay if present
    try:
        cookie_consent_button = driver.find_element(By.XPATH, "//button[contains(text(), 'Accept') or contains(text(), 'Agree')]")
        if cookie_consent_button.is_displayed():
            cookie_consent_button.click()
            logger.info("Clicked on cookie consent button.")
    except NoSuchElementException:
        logger.info("No cookie consent button found.")
    except Exception as e:
        logger.error(f"Error dismissing cookie consent: {e}")

    possible_next_selectors = [
        "ul.pagination li.next a", 
        "ul.pagination li:last-child a",
        "a[aria-label='Next']", 
        "button.next", 
        "button[aria-label='Next']",
        "a.next-page", 
        "a.page-link[rel='next']",
        "//a[contains(text(), 'Next')]",
        "//button[contains(text(), 'Next')]",
        "//span[contains(text(), 'Next')]",
        "//a[contains(@class, 'next') or contains(@id, 'next')]",
        "//button[contains(@class, 'next') or contains(@id, 'next')]",
        "//a[contains(., '›') or contains(., '→') or contains(., '»') or contains(., '>')]",
        "//button[contains(., '›') or contains(., '→') or contains(., '»') or contains(., '>')]",
        "//a[contains(text(), 'Next Page')]",
        "//a[contains(text(), 'Next') and not(contains(@class, 'disabled'))]",
        "//li[contains(@class, 'next')]/a",
        "//li[contains(@class, 'pagination-next')]/a",
        "//a[@class='pagination-next']",
        "//a[@class='next-button']",
        "//a[@class='next-link']",
        "//span[@class='v-btn__content' and @data-no-activator='']"  # New selector added
    ]

    for selector in possible_next_selectors:
        try:
            # Check if the selector is an XPath expression or CSS selector
            if selector.startswith("//"):
                element = driver.find_element(By.XPATH, selector)  # XPath selector
            else:
                element = driver.find_element(By.CSS_SELECTOR, selector)  # CSS selector

            # Scroll into view
            driver.execute_script("arguments[0].scrollIntoView();", element)

            # Wait until the element is clickable
            WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH if selector.startswith("//") else By.CSS_SELECTOR, selector)))

            element.click()
            logger.info(f"Clicked on 'Next' button using selector: {selector}")
            
            # After click, wait for page load to avoid stale element
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
            
            return element

        except (NoSuchElementException, ElementClickInterceptedException, TimeoutException) as e:
            logger.warning(f"Next button not found or not clickable for selector: {selector} - {e}")
            continue
        except StaleElementReferenceException:
            logger.warning(f"Stale element reference after clicking 'Next' button for selector: {selector}. Retrying...")
            continue  # Retry locating the element
        except Exception as e:
            logger.error(f"Unexpected error for selector: {selector} - {e}")
            continue

    return None

def handle_pagination(driver, max_pages=10, timeout=30):
    """
    Function to handle pagination and collect HTML from all pages.
    
    Args:
    driver: Selenium WebDriver instance
    max_pages: Maximum number of pages to scrape (default: 10)
    timeout: Maximum time to wait for elements (default: 30 seconds)
    
    Returns:
    list_of_html_pages: List containing HTML from all scraped pages
    """
    list_of_html_pages = []
    current_page = 1

    while current_page <= max_pages:
        logger.info(f"Processing page {current_page}...")

        try:
            # Wait for the page content to load
            WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )

            # Get the current page's HTML
            page_html = driver.page_source

            # Append the page HTML to the list
            list_of_html_pages.append(page_html)

            # Log current page info
            logger.info(f"Page Title: {driver.title}")

            # Attempt to find the 'Next' button or link
            next_element = find_next_element(driver)

            if next_element and next_element.is_displayed() and next_element.is_enabled():
                # Wait a random time to mimic human behavior
                time.sleep(random.uniform(2, 4))
                
                current_page += 1
            else:
                logger.info("No 'Next' button found. Stopping pagination.")
                break

        except TimeoutException:
            logger.error(f"Timeout occurred while loading page {current_page}")
            break
        except StaleElementReferenceException:
            logger.error(f"Stale element reference on page {current_page}. Retrying...")
            continue  # Retry the loop if element is stale
        except Exception as e:
            logger.error(f"Error during pagination on page {current_page}: {e}")
            break

    return list_of_html_pages


def save_formatted_data(formatted_data, timestamp, output_folder='output'):
    """
    Saves the formatted data as a JSON and Excel file.
    
    Args:
        formatted_data: The data to save.
        timestamp: Timestamp string for file naming.
        output_folder: The directory where the files will be saved.
    
    Returns:
        The path to the saved JSON file.
    """
    try:
        # Ensure the output folder exists
        os.makedirs(output_folder, exist_ok=True)
        
        # Parse the formatted data if it's a JSON string (from some APIs)
        if isinstance(formatted_data, str): 
            try:
                formatted_data_dict = json.loads(formatted_data)
            except json.JSONDecodeError:
                raise ValueError("The provided formatted data is a string but not valid JSON.")
        else:
            # Handle data from APIs that return objects (e.g., Pydantic models)
            formatted_data_dict = formatted_data.dict() if hasattr(formatted_data, 'dict') else formatted_data

        # Save the formatted data as JSON with timestamp in filename
        json_output_path = os.path.join(output_folder, f'sorted_data_{timestamp}.json')
        with open(json_output_path, 'w', encoding='utf-8') as f:
            json.dump(formatted_data_dict, f, ensure_ascii=False, indent=4)
        logger.info(f"Formatted data saved to JSON at {json_output_path}")

        # Prepare data for DataFrame
        if isinstance(formatted_data_dict, dict):
            # If the data is a single dictionary, wrap it in a list
            if all(isinstance(v, dict) for v in formatted_data_dict.values()):
                data_for_df = list(formatted_data_dict.values())
            else:
                # If it's a flat dictionary, convert to a list of one item
                data_for_df = [formatted_data_dict]
        elif isinstance(formatted_data_dict, list):
            data_for_df = formatted_data_dict
        else:
            raise ValueError("Formatted data is neither a dictionary nor a list, cannot convert to DataFrame")

        # Create DataFrame
        try:
            df = pd.DataFrame(data_for_df)
            logger.info("DataFrame created successfully.")

            # Save the DataFrame to an Excel file
            excel_output_path = os.path.join(output_folder, f'sorted_data_{timestamp}.xlsx')
            df.to_excel(excel_output_path, index=False)
            logger.info(f"Formatted data saved to Excel at {excel_output_path}")
            
            return json_output_path  # Return the path to the JSON file

        except Exception as e:
            logger.error(f"Error creating DataFrame or saving Excel: {e}")
            return json_output_path  # Still return JSON path even if Excel fails

    except Exception as e:
        logger.error(f"Failed to save formatted data: {e}")
        raise

def calculate_price(token_counts, model):
    input_token_count = token_counts.get("input_tokens", 0)
    output_token_count = token_counts.get("output_tokens", 0)
    
    # Calculate the costs
    input_cost = input_token_count * PRICING[model]["input"]
    output_cost = output_token_count * PRICING[model]["output"]
    total_cost = input_cost + output_cost
    
    return input_token_count, output_token_count, total_cost

    # ...

if __name__ == "__main__":
    url = 'https://webscraper.io/test-sites/e-commerce/static'
    fields = ['Name of item', 'Price']

    try:
        # Generate timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # Scrape data
        list_of_html_pages = fetch_html_selenium(url)

        all_markdown = ""
        for i, html_content in enumerate(list_of_html_pages, 1):
            markdown = html_to_markdown_with_readability(html_content)
            all_markdown += f"\n\n--- Page {i} ---\n\n" + markdown

        # Save raw data
        save_raw_data(all_markdown, timestamp)

        # Create the dynamic listing model
        DynamicListingModel = create_dynamic_listing_model(fields)

        # Create the container model that holds a list of the dynamic listing models
        DynamicListingsContainer = create_listings_container_model(DynamicListingModel)

        # Format data using the correct model name
        formatted_data, token_counts = format_data(
            all_markdown, 
            DynamicListingsContainer,
            DynamicListingModel,
            "mistral-nemo:latest"  # Updated to match assets.py
        )
        print(formatted_data)
        # Save formatted data
        save_formatted_data(formatted_data, timestamp)

        # Convert formatted_data back to text for token counting
        formatted_data_text = json.dumps(
            formatted_data.dict() if hasattr(formatted_data, 'dict') else formatted_data
        )

        # Automatically calculate the token usage and cost for all input and output
        input_tokens, output_tokens, total_cost = calculate_price(token_counts, "mistral-nemo:latest")
        print(f"Input token count: {input_tokens}")
        print(f"Output token count: {output_tokens}")
        print(f"Estimated total cost: ${total_cost:.4f}")

    except Exception as e:
        print(f"An error occurred: {e}")
    