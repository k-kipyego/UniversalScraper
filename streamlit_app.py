import streamlit as st
from streamlit_tags import st_tags_sidebar
import pandas as pd
import json
import logging
from datetime import datetime
from scraper import fetch_html_selenium, save_raw_data, format_data, save_formatted_data, calculate_price, html_to_markdown_with_readability, create_dynamic_listing_model, create_listings_container_model
from database_push import push_json_to_db, push_website_info_to_db  # Update this import
from assets import PRICING

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Example usage of logger
logger.info("This is an info message")

# Initialize Streamlit app
st.set_page_config(page_title="Universal Web Scraper")
st.title("Universal Web Scraper 🦑")

# Mapping of website names to URLs
WEBSITE_URLS = {
    "PPIP": "https://tenders.go.ke/tenders",
    "Nigeria Government": "https://etenders.com.ng/#:~:text=Advertise%20your%20procurement%20needs%20and%20connect%20with%20a%20wide%20array",
    "South Africa Government": "https://www.etenders.gov.za/Home/opportunities?id=1",
    "Senegal Tenders": "https://www.senegaltenders.com/computer-and-related-services-tenders.php"
}

# Predefined tags for each website
PREDEFINED_TAGS = {
    "https://tenders.go.ke/tenders": ["Tender No", "Description", "Category", "Deadline", "Location"],
    "https://etenders.com.ng/#:~:text=Advertise%20your%20procurement%20needs%20and%20connect%20with%20a%20wide%20array": ["Title", "Date Added", "Deadline", "Category"],
    "https://www.etenders.gov.za/Home/opportunities?id=1": ["Category", "Description", "Added", "Deadline"],
    "https://www.senegaltenders.com/computer-and-related-services-tenders.php": ["Title", "Ref No", "Deadline"]
}

# Sidebar components
st.sidebar.title("Web Scraper Settings")

# Dropdown to select the website by name
selected_website_name = st.sidebar.selectbox("Select Website", options=list(WEBSITE_URLS.keys()))

# Get the corresponding URL for the selected website name
selected_website_url = WEBSITE_URLS[selected_website_name]

# Populate tags based on the selected website URL
selected_tags = PREDEFINED_TAGS[selected_website_url]

# Display the selected tags
st.sidebar.markdown("### Selected Tags")
st.sidebar.write(selected_tags)

# Other sidebar components
model_selection = st.sidebar.selectbox("Select Model", options=list(PRICING.keys()), index=0)
url_input = st.sidebar.text_input("Enter URL", value=selected_website_url)
max_pages = st.sidebar.number_input("Number of Pages to Scrape", min_value=1, max_value=100, value=3, step=1)

# Dropdown for predefined labels
selected_labels = st.sidebar.multiselect(
    "Select Fields to Extract:",
    options=selected_tags,
    default=selected_tags  # Default to all tags for the selected website
)

input_tokens = output_tokens = total_cost = 0  # Default values

# Define the scraping function
def perform_scrape():
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    raw_html = fetch_html_selenium(url_input, max_pages=int(max_pages))
    markdown = html_to_markdown_with_readability(raw_html)
    raw_file_path = save_raw_data(markdown, timestamp)
    DynamicListingModel = create_dynamic_listing_model(selected_labels)
    DynamicListingsContainer = create_listings_container_model(DynamicListingModel)
    formatted_data, tokens_count = format_data(markdown, DynamicListingsContainer, DynamicListingModel, model_selection)
    input_tokens, output_tokens, total_cost = calculate_price(tokens_count, model=model_selection)
    formatted_json_path = save_formatted_data(formatted_data, timestamp)  # Receives JSON path
    
    # Convert DynamicListingsContainer to a dictionary
    formatted_data_dict = formatted_data.dict()
    
    return formatted_data_dict, markdown, input_tokens, output_tokens, total_cost, timestamp, formatted_json_path

def create_dataframe(data):
    print("Debug: Type of data:", type(data))
    print("Debug: Content of data:", data)

    if isinstance(data, dict):
        df = pd.DataFrame([data])
    elif isinstance(data, list):
        if all(isinstance(item, dict) for item in data):
            df = pd.DataFrame(data)
        else:
            df = pd.DataFrame(data, columns=['data'])
    elif isinstance(data, str):
        try:
            parsed_data = json.loads(data)
            if isinstance(parsed_data, dict):
                df = pd.DataFrame([parsed_data])
            elif isinstance(parsed_data, list):
                df = pd.DataFrame(parsed_data)
            else:
                df = pd.DataFrame([{'data': data}])
        except json.JSONDecodeError:
            df = pd.DataFrame([{'data': data}])
    else:
        df = pd.DataFrame([{'data': str(data)}])

    print("Debug: DataFrame shape:", df.shape)
    print("Debug: DataFrame columns:", df.columns)
    print("Debug: First few rows of DataFrame:")
    print(df.head())

    return df

if 'perform_scrape' not in st.session_state:
    st.session_state['perform_scrape'] = False

if st.sidebar.button("Scrape"):
    with st.spinner('Please wait... Data is being scraped and pushed to the database.'):
        try:
            formatted_data, markdown, input_tokens, output_tokens, total_cost, timestamp, formatted_json_path = perform_scrape()
            
            # Push the JSON data to PostgreSQL
            push_json_to_db(formatted_json_path, table_name='scraped_data')
            
            # Push website information to PostgreSQL
            try:
                push_website_info_to_db(
                    url=url_input,
                    name=selected_website_name,
                    labels=selected_labels,
                    table_name='website_info'
                )
                st.success("Website information successfully saved to the database.")
            except Exception as e:
                st.error(f"Failed to save website information to the database: {e}")
                logger.error(f"Database insertion error: {e}", exc_info=True)
            
            st.session_state['results'] = (formatted_data, markdown, input_tokens, output_tokens, total_cost, timestamp)
            st.session_state['perform_scrape'] = True
        except Exception as e:
            st.error(f"An error occurred during scraping or database insertion: {e}")
            logger.error(f"Scraping error: {e}", exc_info=True)
            st.session_state['perform_scrape'] = False

if st.session_state.get('perform_scrape'):
    formatted_data, markdown, input_tokens, output_tokens, total_cost, timestamp = st.session_state['results']
    
    # Display the formatted data
    st.write("Scraped Data:")
    st.json(formatted_data)
    
    st.sidebar.markdown("## Token Usage")
    st.sidebar.markdown(f"**Input Tokens:** {input_tokens}")
    st.sidebar.markdown(f"**Output Tokens:** {output_tokens}")
    st.sidebar.markdown(f"**Total Cost:** :green-background[***${total_cost:.4f}***]")

    # Create columns for download buttons
    col1, col2, col3 = st.columns(3)
    with col1:
        st.download_button(
            "Download JSON",
            data=json.dumps(formatted_data, indent=4),
            file_name=f"{timestamp}_data.json"
        )
    with col2:
        # Convert formatted data to a DataFrame regardless of its structure
        try:
            df_display = create_dataframe(formatted_data)
            st.download_button(
                "Download CSV",
                data=df_display.to_csv(index=False),
                file_name=f"{timestamp}_data.csv"
            )
        except Exception as e:
            st.error(f"Failed to create DataFrame for CSV download: {e}")
    with col3:
        st.download_button(
            "Download Markdown",
            data=markdown,
            file_name=f"{timestamp}_data.md"
        )