import os
import random
import time
import re
import json
from datetime import datetime
from typing import List, Dict, Type

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
from typing import Optional
from urllib.parse import urljoin

from openai import OpenAI
import google.generativeai as genai
from groq import Groq

from assets import USER_AGENTS,PRICING,HEADLESS_OPTIONS,SYSTEM_MESSAGE,USER_MESSAGE,LLAMA_MODEL_FULLNAME,GROQ_LLAMA_MODEL_FULLNAME
load_dotenv()

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
                    element = driver.find_element(By.XPATH, f"//{tag}[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{text}')]" )
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
        time.sleep(random.uniform(1, 2))
        driver.maximize_window()
        
        # Get the base URL for making relative URLs absolute
        base_url = driver.current_url
        
        # Execute JavaScript to collect all procurement notice links
        js_script = """
        function findProcurementLinks() {
            const links = [];
            const patterns = [
                /tender/i, /procurement/i, /bid/i, /rfp/i, /proposal/i,
                /notice/i, /detail/i, /view/i, /more/i
            ];
            
            document.querySelectorAll('a').forEach(link => {
                const href = link.href;
                const text = link.textContent.toLowerCase();
                const onClick = link.getAttribute('onclick');
                const dataAttrs = Array.from(link.attributes)
                    .filter(attr => attr.name.startsWith('data-'))
                    .map(attr => attr.value);
                
                if (patterns.some(pattern => 
                    pattern.test(href) || 
                    pattern.test(text) ||
                    (onClick && pattern.test(onClick)) ||
                    dataAttrs.some(attr => pattern.test(attr))
                )) {
                    links.push(href);
                }
            });
            return links;
        }
        return findProcurementLinks();
        """
        procurement_links = driver.execute_script(js_script)
        html = driver.page_source
        
        return {
            'html': html,
            'base_url': base_url,
            'procurement_links': procurement_links
        }
    finally:
        driver.quit()

def clean_html(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Remove headers and footers based on common HTML tags or classes
    for element in soup.find_all(['header', 'footer']):
        element.decompose()  # Remove these tags and their content

    return str(soup)


def html_to_markdown_with_readability(html_content, base_url=None):
    """Convert HTML to markdown while preserving URLs and making them absolute."""
    # Check if html_content is a tuple (from fetch_html_selenium)
    if isinstance(html_content, tuple):
        html_string, base_url, procurement_links = html_content
    else:
        html_string = html_content
        procurement_links = []

    # Create BeautifulSoup object
    soup = BeautifulSoup(html_string, 'html.parser')
    
    # Make all URLs absolute
    if base_url:
        for tag in soup.find_all(['a', 'link'], href=True):
            tag['href'] = urljoin(base_url, tag['href'])
    
    # Add procurement links as data attribute if they exist
    if procurement_links:
        soup.body['data-procurement-links'] = json.dumps(procurement_links)
    
    cleaned_html = clean_html(str(soup))
    
    # Convert to markdown while preserving URLs
    markdown_converter = html2text.HTML2Text()
    markdown_converter.ignore_links = False
    markdown_converter.wrap_links = False
    markdown_content = markdown_converter.handle(cleaned_html)
    
    return markdown_content


def save_raw_data(raw_data: str, output_folder: str, file_name: str):
    """Save raw markdown data to the specified output folder."""
    os.makedirs(output_folder, exist_ok=True)
    raw_output_path = os.path.join(output_folder, file_name)
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


def create_dynamic_listing_model(fields: List[str]):
    """Creates a dynamic Pydantic model based on the provided fields."""
    field_definitions = {
        field: (Optional[str], None) for field in fields
    }
    # Add direct_url field to capture specific procurement notice URL
    field_definitions['direct_url'] = (Optional[str], Field(None, description="Direct URL to the specific procurement notice"))
    
    return create_model('DynamicListing', **field_definitions)

def create_listings_container_model(listing_model: Type[BaseModel]) -> Type[BaseModel]:
    """
    Create a container model that holds a list of the given listing model.
    """
    container_model = create_model('DynamicListingsContainer', listings=(List[listing_model], ...))
    
    # Add a method to serialize the container to a dictionary
    def to_dict(self):
        return {"listings": [listing.dict() for listing in self.listings]}
    
    container_model.to_dict = to_dict
    return container_model


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
     You are an intelligent text extraction and conversion assistant. Your task is to extract structured information from the given 
     text and convert it into a pure JSON format. Focus specifically on procurement opportunities related to software and IT solutions, including custom software development, 
     ERP systems (financial, HR, supply chain), IT consulting and advisory services, digital skills training, and capacity building. 
     Additionally, include government and enterprise solutions such as Public Financial Management (budgeting, treasury, revenue), Identity Management (national ID, biometrics), 
     Tax/Customs platforms (revenue collection, debt recovery),Robotics Process Automation, Business Process Outsourcing services, and related technical services like system integration, cloud solutions, and data management. 
     Only extract information that explicitly mentions these areas or related keywords such as 'software', 'IT', 'cloud', 'data management', 'cybersecurity', and 'system integration'. 
     If you encounter any content that is not in English, translate it into English before extracting the relevant information. 
     Provide output in pure JSON format with no additional commentary, and ensure the output strictly follows this schema:
 
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
    
    if selected_model in ["gpt-4o-mini", "gpt-4o-2024-08-06"]:
        # Use OpenAI API
        client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
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
        return completion.choices[0].message.parsed, token_counts

    elif selected_model == "gemini-1.5-flash":
        try:
            # Configure Gemini
            genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))
            model = genai.GenerativeModel('gemini-1.5-flash',
                    generation_config={
                        "temperature": 0.2,
                        "top_p": 0.7,
                        "top_k": 20,
                        "max_output_tokens": 2048,
                    })

            # Enhanced system message with explicit filtering instructions and translation capability
            base_sys_message = generate_system_message(DynamicListingModel)
            enhanced_sys_message = f"""
            {base_sys_message}

            IMPORTANT EXTRACTION AND TRANSLATION RULES:
            1. LANGUAGE HANDLING:
               - Process content in ANY language
               - Translate all extracted information to English
               - Maintain the original meaning and technical terminology
               - Include the original language version of the title in a new field 'original_title'
               - If the listing is not in English, add a field 'source_language' specifying the original language

            2. ONLY extract information about opportunities that EXPLICITLY mention:
               - Software development or IT solutions
               - ERP systems (financial, HR, supply chain)
               - IT consulting and advisory services
               - Digital skills training
               - Government technology solutions
               - System integration
               - Cloud solutions
               - Data management
               - Cybersecurity
               - Digital transformation

            3. SKIP any opportunity that:
               - Does not explicitly mention technology or IT services
               - Is purely about physical goods or non-IT services
               - Is ambiguous about IT involvement

            4. For each extracted opportunity, you MUST be able to point to specific text that confirms it's IT/software related.

            FORMAT: Provide output in pure JSON following the schema exactly. Do not include explanatory text.
            For non-English listings, include:
            - 'original_title': The title in original language
            - 'source_language': The language code (e.g., 'es', 'fr', 'de')

            BEFORE INCLUDING ANY LISTING, ASK:
            - Does it explicitly mention IT/software services?
            - Is it clearly a technology procurement?
            - Can I point to specific IT-related keywords in the text?
            - Have I accurately translated all relevant information to English?
            - Have I preserved the original title for non-English listings?

            Only include listings that pass these checks.
            """

            # Function to validate if a response meets IT criteria
            def validate_it_relevance(response_dict):
                if not isinstance(response_dict, dict) or "listings" not in response_dict:
                    return response_dict
                
                it_keywords = {
                    'software', 'it ', 'ict', 'technology', 'digital', 'system', 
                    'cloud', 'cyber', 'erp', 'data', 'integration', 'platform',
                    'application', 'network', 'security', 'automation', 'api',
                    'development', 'database', 'infrastructure', 'computing'
                }
                
                # Add common IT keywords in other languages
                multilingual_it_keywords = {
                    # Spanish
                    'informática', 'desarrollo', 'tecnología', 'sistema', 'nube',
                    # French
                    'informatique', 'développement', 'technologie', 'système', 'nuage',
                    # German
                    'informatik', 'entwicklung', 'technologie', 'system', 'cloud',
                    # Add more languages as needed
                }
                
                it_keywords.update(multilingual_it_keywords)
                
                filtered_listings = []
                for listing in response_dict["listings"]:
                    # Convert all text fields to lowercase for checking
                    listing_text = ' '.join(str(v).lower() for v in listing.values())
                    # Check if any IT keyword is present
                    if any(keyword in listing_text for keyword in it_keywords):
                        # Ensure required translation fields are present for non-English listings
                        if 'source_language' in listing and listing['source_language'] != 'en':
                            if 'original_title' not in listing:
                                listing['original_title'] = listing.get('title', 'No title available')
                        filtered_listings.append(listing)
                
                return {"listings": filtered_listings}

            prompt = f"{enhanced_sys_message}\n\n{USER_MESSAGE}{data}"
            input_token_count = model.count_tokens(prompt).total_tokens
            
            # Generate completion with additional logging
            print("Sending request to Gemini...")
            completion = model.generate_content(prompt)
            print(f"Received response from Gemini. Response type: {type(completion)}")
            
            if not completion or not completion.text:
                print("Empty response received from Gemini")
                raise ValueError("Empty response from Gemini API")
                
            response_content = completion.text.strip()
            print(f"Raw response content: {response_content[:500]}...")  # Print first 500 chars for debugging
            
            # Add validation before JSON parsing
            if not response_content:
                raise ValueError("Empty response content")
                
            # Try to clean the response if it contains markdown code blocks
            if response_content.startswith("```") and response_content.endswith("```"):
                response_content = response_content.replace("```json", "").replace("```", "").strip()
            
            try:
                parsed_response = json.loads(response_content)
            except json.JSONDecodeError as je:
                print(f"JSON parsing error: {str(je)}")
                print(f"Problematic content: {response_content}")
                # Attempt to provide a valid fallback response
                parsed_response = {"listings": []}
            # Apply IT relevance filtering
            filtered_response = validate_it_relevance(parsed_response)
            
            # Create response object with the filtered data
            class FormattedResponse:
                def __init__(self, data):
                    self.data = data
                def to_dict(self):
                    return self.data
                def dict(self):
                    return self.data

            try:
                output_token_count = model.count_tokens(json.dumps(filtered_response)).total_tokens
            except:
                output_token_count = len(json.dumps(filtered_response)) // 4

            token_counts = {
                "input_tokens": input_token_count,
                "output_tokens": output_token_count
            }

            return FormattedResponse(filtered_response), token_counts

        except Exception as e:
            print(f"Error processing Gemini model: {str(e)}")
        print(f"Full error details:")
        import traceback
        print(traceback.format_exc())
        # Return a valid but empty response instead of None
        return FormattedResponse({"listings": []}), {"input_tokens": 0, "output_tokens": 0}
    
    elif selected_model == "Groq Llama3.1 70b":
        try:
            # Dynamically generate the system message based on the schema
            sys_message = generate_system_message(DynamicListingModel)

            # Initialize Groq client
            client = Groq(api_key=os.environ.get("GROQ_API_KEY"))

            # Generate the completion response
            completion = client.chat.completions.create(
                messages=[
                    {"role": "system", "content": sys_message},
                    {"role": "user", "content": USER_MESSAGE + data}
                ],
                model=GROQ_LLAMA_MODEL_FULLNAME
            )

            # Validate response
            if not completion or not completion.choices:
                raise ValueError("Empty or malformed response from Groq API")

            response_content = completion.choices[0].message.content
            print("Raw Groq Response:", response_content)

            # Parse JSON response
            try:
                parsed_response = json.loads(response_content)
                
                # Standardize the response format
                if isinstance(parsed_response, list):
                    formatted_data = {"listings": parsed_response}
                elif isinstance(parsed_response, dict):
                    if "listings" not in parsed_response:
                        formatted_data = {"listings": [parsed_response]}
                    else:
                        formatted_data = parsed_response
                else:
                    raise ValueError(f"Unexpected response type: {type(parsed_response)}")
                
            except json.JSONDecodeError:
                raise ValueError(f"Invalid JSON response: {response_content}")

            # Extract token usage
            token_counts = {
                "input_tokens": getattr(completion.usage, 'prompt_tokens', 0),
                "output_tokens": getattr(completion.usage, 'completion_tokens', 0)
            }

            # Create a dict-like object with to_dict method
            class FormattedResponse:
                def __init__(self, data):
                    self.data = data
                def to_dict(self):
                    return self.data

            return FormattedResponse(formatted_data), token_counts
        
        except Exception as e:
            print(f"Error processing Groq model: {str(e)}")
            import traceback
            print(traceback.format_exc())
            return None, None

def save_formatted_data(formatted_data, output_folder: str, json_file_name: str, excel_file_name: str):
    """Save formatted data as JSON and Excel in the specified output folder."""
    os.makedirs(output_folder, exist_ok=True)
    
    # Handle different types of formatted data
    if isinstance(formatted_data, str):
        try:
            formatted_data_dict = json.loads(formatted_data)
        except json.JSONDecodeError:
            raise ValueError("The provided formatted data is a string but not valid JSON.")
    elif hasattr(formatted_data, 'to_dict'):
        formatted_data_dict = formatted_data.to_dict()
    elif isinstance(formatted_data, (dict, list)):
        formatted_data_dict = formatted_data
    else:
        raise ValueError(f"Unsupported data type: {type(formatted_data)}")

    # Save JSON
    json_output_path = os.path.join(output_folder, json_file_name)
    with open(json_output_path, 'w', encoding='utf-8') as f:
        json.dump(formatted_data_dict, f, indent=4)
    print(f"Formatted data saved to JSON at {json_output_path}")

    # Prepare data for DataFrame
    if isinstance(formatted_data_dict, dict):
        if "listings" in formatted_data_dict:
            data_for_df = formatted_data_dict["listings"]
        else:
            data_for_df = [formatted_data_dict]
    elif isinstance(formatted_data_dict, list):
        data_for_df = formatted_data_dict
    else:
        raise ValueError("Cannot convert data to DataFrame")

    # Create and save DataFrame
    try:
        df = pd.DataFrame(data_for_df)
        excel_output_path = os.path.join(output_folder, excel_file_name)
        df.to_excel(excel_output_path, index=False)
        print(f"Formatted data saved to Excel at {excel_output_path}")
        return df
    except Exception as e:
        print(f"Error creating DataFrame or saving Excel: {str(e)}")
        return None

def scrape_url(url: str, fields: List[str], selected_model: str, output_folder: str, file_number: int, markdown: str):
    """Scrape a single URL and save the results."""
    try:
        # Save raw data
        save_raw_data(markdown, output_folder, f'rawData_{file_number}.md')

        # Create the dynamic listing model
        DynamicListingModel = create_dynamic_listing_model(fields)
        DynamicListingsContainer = create_listings_container_model(DynamicListingModel)
        
        # Format data
        formatted_data, token_counts = format_data(markdown, DynamicListingsContainer, DynamicListingModel, selected_model)
        
        if formatted_data is None:
            raise ValueError("Failed to format data")

        # Save formatted data
        save_formatted_data(formatted_data, output_folder, f'sorted_data_{file_number}.json', f'sorted_data_{file_number}.xlsx')

        # Calculate token usage and cost
        input_tokens, output_tokens, total_cost = calculate_price(token_counts, selected_model)
        
        # Return the formatted data in the correct format
        return input_tokens, output_tokens, total_cost, formatted_data.to_dict() if hasattr(formatted_data, 'to_dict') else formatted_data

    except Exception as e:
        print(f"An error occurred while processing {url}: {str(e)}")
        return 0, 0, 0, None

def calculate_price(token_counts, model):
    input_token_count = token_counts.get("input_tokens", 0)
    output_token_count = token_counts.get("output_tokens", 0)
    
    # Calculate the costs
    input_cost = input_token_count * PRICING[model]["input"]
    output_cost = output_token_count * PRICING[model]["output"]
    total_cost = input_cost + output_cost
    
    return input_token_count, output_token_count, total_cost


def generate_unique_folder_name(url):
    timestamp = datetime.now().strftime('%Y_%m_%d__%H_%M_%S')
    url_name = re.sub(r'\W+', '_', url.split('//')[1].split('/')[0])  # Extract domain name and replace non-alphanumeric characters
    return f"{url_name}_{timestamp}"


def scrape_multiple_urls(urls, fields, selected_model):
    output_folder = os.path.join('output', generate_unique_folder_name(urls[0]))
    os.makedirs(output_folder, exist_ok=True)
    
    total_input_tokens = 0
    total_output_tokens = 0
    total_cost = 0
    all_data = []
    markdown = None  # We'll store the markdown for the first (or only) URL
    
    for i, url in enumerate(urls, start=1):
        raw_html = fetch_html_selenium(url)
        current_markdown = html_to_markdown_with_readability(raw_html)
        if i == 1:
            markdown = current_markdown  # Store markdown for the first URL
        
        input_tokens, output_tokens, cost, formatted_data = scrape_url(url, fields, selected_model, output_folder, i, current_markdown)
        total_input_tokens += input_tokens
        total_output_tokens += output_tokens
        total_cost += cost
        all_data.append(formatted_data)
    
    return output_folder, total_input_tokens, total_output_tokens, total_cost, all_data, markdown

def scrape_url(url: str, fields: List[str], selected_model: str, output_folder: str, file_number: int, markdown: str):
    """Scrape a single URL and save the results."""
    try:
        # Save raw data
        save_raw_data(markdown, output_folder, f'rawData_{file_number}.md')

        # Create the dynamic listing model
        DynamicListingModel = create_dynamic_listing_model(fields)

        # Create the container model that holds a list of the dynamic listing models
        DynamicListingsContainer = create_listings_container_model(DynamicListingModel)
        
        # Format data
        formatted_data, token_counts = format_data(markdown, DynamicListingsContainer, DynamicListingModel, selected_model)
        
        # Save formatted data
        save_formatted_data(formatted_data, output_folder, f'sorted_data_{file_number}.json', f'sorted_data_{file_number}.xlsx')

        # Calculate and return token usage and cost
        input_tokens, output_tokens, total_cost = calculate_price(token_counts, selected_model)
        return input_tokens, output_tokens, total_cost, formatted_data.to_dict()  # Use to_dict here

    except Exception as e:
        print(f"An error occurred while processing {url}: {e}")
        return 0, 0, 0, None