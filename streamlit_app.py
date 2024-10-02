import streamlit as st
from streamlit_tags import st_tags_sidebar
import pandas as pd
import json
import logging
from datetime import datetime
from scraper import fetch_html_selenium, save_raw_data, format_data, save_formatted_data, calculate_price, html_to_markdown_with_readability, create_dynamic_listing_model, create_listings_container_model  
from database_push import push_json_to_db, push_website_info_to_db 
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
    "UNGM" : "https://www.ungm.org/Public/Notice",
    "IOM": "https://www.iom.int/procurement-opportunities",
    "Malawi": "https://www.ppda.mw/tenders",
    "UNDP": "https://procurement-notices.undp.org/#:~:text=RFP/JSB-AC/2409/52%20Develop%20a%20national%20e-procurement",
    "AFDB": "https://www.afdb.org/en/projects-and-operations/procurement#:~:text=Procurement%20procedures%20must%20offer%20equal%20opportunities%20to",
    "KRA" : "https://www.kra.go.ke/tenders#:~:text=E%20-%20Procurement%20We%20are%20always%20working%20closely%20with%20our",
    "Swaziland": "https://esppra.co.sz/sppra/tender.php",
    "Nigeria" : "https://www.publicprocurement.ng/#:~:text=ministry%20for%20local%20government%20and%20chieftaincy%20affairs,%20yobe",
    "Uganda": "https://gpp.ppda.go.ug/public/bid-invitations",
    "EC": "https://ec.europa.eu/info/funding-tenders/opportunities/portal/screen/opportunities/calls-for-tenders?keywords=software&isExactMatch=true&order=DESC&pageNumber=1&pageSize=50&sortBy=startDate",
    "Georgia": "https://ssl.doas.state.ga.us/gpr/",
    "EIA" : "https://www.eib.org/en/about/procurement/all/index.htm?q=&sortColumn=configuration.contentStart&sortDir=desc&pageNumber=0&itemPerPage=25&pageable=true&la=EN&deLa=EN&yearTo=&orYearTo=true&yearFrom=&orYearFrom=true&procurementStatus=&or_g_procurementInformations_type=true",
    "UN": "https://www.un.org/Depts/ptd/eoi",
    "DepEd": "https://depedpines.com/procurement-notices/",
    "GeBiz": "https://www.gebiz.gov.sg/ptn/opportunity/BOListing.xhtml?origin=menu",
    "Mauritius": "https://publicprocurement.govmu.org/publicprocurement/?page_id=720",
    "Bermuda": "https://www.gov.bm/procurement-notices",
    "Caribbean Bank": "https://www.caribank.org/work-with-us/procurement/general-procurement-notices",
    "Hong Kong": "https://pcms2.gld.gov.hk/iprod/#/sta00305?lang-setting=en-US&results_pageNo=1",
    "Aus Tender": "https://www.tenders.gov.au/atm",
    "Sri Lanka": "https://www.slcgmel.org/procurement-notices/",
    "ADB": "https://www.adb.org/projects/tenders/group/goods",
    "HANDS": "https://hands.ehawaii.gov/hands/opportunities",
    "GoC": "https://canadabuys.canada.ca/en/tender-opportunities",
    "Scotland": "https://www.publiccontractsscotland.gov.uk/Search/Search_MainPage.aspx"

}

UNIVERSAL_LABELS = [
    "Title", "Description", "Date Posted", "Deadline", "Reference Number",
    "Category", "Location", "Language", "Contact", "Budget", "Type"
]

# Predefined tags for each website (can include both universal and specific labels)
PREDEFINED_TAGS = {
    "https://tenders.go.ke/tenders": ["Tender No", "Description", "Category", "Deadline", "Location"],
    "https://www.ungm.org/Public/Notice": ["Title", "Category", "Date Posted", "Deadline", "Type", "Location"],
    "https://www.iom.int/procurement-opportunities": ["Title", "Category", "Date Posted", "Deadline", "Type", "Location"],
    "https://www.ppda.mw/tenders": ["Title", "Category", "Date Posted", "Deadline", "Reference Number"],
    "https://procurement-notices.undp.org/#:~:text=RFP/JSB-AC/2409/52%20Develop%20a%20national%20e-procurement": ["Title", "Ref No", "Date Posted", "Deadline", "Type", "Location"],
    "https://www.afdb.org/en/projects-and-operations/procurement#:~:text=Procurement%20procedures%20must%20offer%20equal%20opportunities%20to": ["Title", "Date Posted"],
    "https://www.kra.go.ke/tenders#:~:text=E%20-%20Procurement%20We%20are%20always%20working%20closely%20with%20our": ["Title", "Date Posted", "Deadline"],
    "https://esppra.co.sz/sppra/tender.php": ["Title", "Ref No", "Deadline", "Date Posted"],
    "https://www.publicprocurement.ng/#:~:text=ministry%20for%20local%20government%20and%20chieftaincy%20affairs,%20yobe": ["Description", "Date Added", "Deadline", "Type"],
    "https://gpp.ppda.go.ug/public/bid-invitations": ["Title", "Deadline", "Type"],
    "https://ec.europa.eu/info/funding-tenders/opportunities/portal/screen/opportunities/calls-for-tenders?keywords=software&isExactMatch=true&order=DESC&pageNumber=1&pageSize=50&sortBy=startDate": ["Title", "Deadline", "Type", "Status", "Date Posted"],
    "https://ssl.doas.state.ga.us/gpr/": ["Title", "Ref No", "Status", "Deadline", "Date Posted"],
    "https://www.eib.org/en/about/procurement/all/index.htm?q=&sortColumn=configuration.contentStart&sortDir=desc&pageNumber=0&itemPerPage=25&pageable=true&la=EN&deLa=EN&yearTo=&orYearTo=true&yearFrom=&orYearFrom=true&procurementStatus=&or_g_procurementInformations_type=true": ["Title", "Type", "Status", "Date Posted"],
    "https://www.un.org/Depts/ptd/eoi" : ["Title","Date Posted", "Deadline", "Reference Number"],
    "https://depedpines.com/procurement-notices/" : ["Title", "Date Posted", "Deadline"],
    "https://www.gebiz.gov.sg/ptn/opportunity/BOListing.xhtml?origin=menu" : ["Ref No", "Title", "Date Posted", "Deadline", "Cartegory", "Status"],
    "https://publicprocurement.govmu.org/publicprocurement/?page_id=720":  ["Description", "Reference Number", "Deadline", "Cartegory",],
    "https://www.gov.bm/procurement-notices":   ["Title", "Date Posted", "Deadline", "Ref No", ],
    "https://www.caribank.org/work-with-us/procurement/general-procurement-notices": ["Title", "Cartegory", "Location"],
    "https://pcms2.gld.gov.hk/iprod/#/sta00305?lang-setting=en-US&results_pageNo=1" : ["Description", "Deadline", "Ref No", "Cartegory"],
    "https://www.tenders.gov.au/atm" : ["Description", "Deadline", "Cartegory", "Ref No",],
    "https://www.slcgmel.org/procurement-notices/" : ["Title", "Date Posted", "Type"],
    "https://www.adb.org/projects/tenders/group/goods" : ["Title", "Date Posted", "Deadline", "Type", "Ref No", "Status"],
    "https://hands.ehawaii.gov/hands/opportunities" : ["Title", "Location", "Deadline", "Cartegory", "Ref No", "Status", "Date Posted"],
    "https://canadabuys.canada.ca/en/tender-opportunities" : ["Title", "Cartegory", "Deadline", "Date Posted"],
    "https://www.publiccontractsscotland.gov.uk/Search/Search_MainPage.aspx" : ["Title", "Ref No", "Deadline", "Date Posted", "Type"],

}
# Sidebar components
st.sidebar.title("Web Scraper Settings")

# Dropdown to select the website by name
selected_website_name = st.sidebar.selectbox("Select Website", options=list(WEBSITE_URLS.keys()))

# Get the corresponding URL for the selected website name
selected_website_url = WEBSITE_URLS[selected_website_name]

# Combine universal labels with predefined tags for the selected website
combined_labels = list(set(UNIVERSAL_LABELS + PREDEFINED_TAGS.get(selected_website_url, [])))

# Display the combined labels



# Other sidebar components
model_selection = st.sidebar.selectbox("Select Model", options=list(PRICING.keys()), index=0)
url_input = st.sidebar.text_input("Enter URL", value=selected_website_url)
max_pages = st.sidebar.number_input("Number of Pages to Scrape", min_value=1, max_value=100, value=3, step=1)

# Dropdown for selecting labels
selected_labels = st.sidebar.multiselect(
    "Select Fields to Extract:",
    options=combined_labels,
    default=PREDEFINED_TAGS.get(selected_website_url, UNIVERSAL_LABELS[:5])  # Default to predefined tags or first 5 universal labels
)

input_tokens = output_tokens = total_cost = 0  # Default values

# Define the scraping function
def perform_scrape():
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    raw_html_list = fetch_html_selenium(url_input, max_pages=int(max_pages))
    
    # Concatenate all HTML pages into a single string
    raw_html = "\n".join(raw_html_list)  # Join the list into a single string
    
    markdown = html_to_markdown_with_readability(raw_html)
    raw_file_path = save_raw_data(markdown, timestamp)
    
    # Create dynamic models
    DynamicListingModel = create_dynamic_listing_model(selected_labels)
    DynamicListingsContainer = create_listings_container_model(DynamicListingModel)
    
    # Format data
    formatted_data, tokens_count = format_data(markdown, DynamicListingsContainer, DynamicListingModel, model_selection)
    
    # Check if formatted_data is already a dict, if not, convert it
    if isinstance(formatted_data, dict):
        formatted_data_dict = formatted_data  # It's already a dict
    elif hasattr(formatted_data, 'dict'):
        formatted_data_dict = formatted_data.dict()  # Convert if it has a dict method
    else:
        raise ValueError("Formatted data is not in an expected format.")

    input_tokens, output_tokens, total_cost = calculate_price(tokens_count, model=model_selection)
    formatted_json_path = save_formatted_data(formatted_data_dict, timestamp)  # Receives JSON path
    
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
            push_json_to_db(formatted_json_path, table_name='scraped_data', website_name=selected_website_name, website_url=selected_website_url)
            
            # Push website information to PostgreSQL
            try:
                push_website_info_to_db(
                    website_url=selected_website_url,
                    website_name=selected_website_name,
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