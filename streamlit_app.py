import streamlit as st
from streamlit_tags import st_tags_sidebar
import pandas as pd
import json
import logging
from datetime import datetime
from scraper import fetch_html_selenium, save_raw_data, format_data, save_formatted_data, calculate_price, html_to_markdown_with_readability, create_dynamic_listing_model, create_listings_container_model, scrape_url
from pagination_detector import detect_pagination_elements, PaginationData
from assets import PRICING
import os
from pydantic import BaseModel
from urllib.parse import urlparse
import re
from dotenv import load_dotenv  # Add this import for loading .env variables
import time  # Add this import at the top of the file
from database_push import push_json_to_db, push_website_info_to_db 



load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

file_handler = logging.FileHandler('app.log')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)

def serialize_pydantic(obj):
    if isinstance(obj, BaseModel):
        return obj.dict()
    raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')

# Initialize Streamlit app
st.set_page_config(page_title="Universal Web Scraper", page_icon="🦑")
st.title("Universal Web Scraper 🦑")

# Initialize session state variables if they don't exist
if 'results' not in st.session_state:
    st.session_state['results'] = None
if 'perform_scrape' not in st.session_state:
    st.session_state['perform_scrape'] = False

# Mapping of website names to URLs
WEBSITE_URLS = {
    # "PPIP": "https://tenders.go.ke/tenders",
    # "UNGM": "https://www.ungm.org/Public/Notice",
    "IOM": "https://www.iom.int/procurement-opportunities",
    "Malawi": "https://www.ppda.mw/tenders",
    # "UNDP": "https://procurement-notices.undp.org/#:~:text=RFP/JSB-AC/2409/52%20Develop%20a%20national%20e-procurement",
    "AFDB": "https://www.afdb.org/en/projects-and-operations/procurement#:~:text=Procurement%20procedures%20must%20offer%20equal%20opportunities%20to",
    "KRA": "https://www.kra.go.ke/tenders#:~:text=E%20-%20Procurement%20We%20are%20always%20working%20closely%20with%20our",
    "Swaziland": "https://esppra.co.sz/sppra/tender.php",
    "Nigeria": "https://www.publicprocurement.ng/#:~:text=ministry%20for%20local%20government%20and%20chieftaincy%20affairs,%20yobe",
    "Uganda": "https://gpp.ppda.go.ug/public/bid-invitations",
    "EC": "https://ec.europa.eu/info/funding-tenders/opportunities/portal/screen/opportunities/calls-for-tenders?keywords=software&isExactMatch=true&order=DESC&pageNumber=1&pageSize=50&sortBy=startDate",
    "Georgia": "https://ssl.doas.state.ga.us/gpr/",
    "EIB": "https://www.eib.org/en/about/procurement/all/index.htm?q=&sortColumn=configuration.contentStart&sortDir=desc&pageNumber=0&itemPerPage=25&pageable=true&la=EN&deLa=EN&yearTo=&orYearTo=true&yearFrom=&orYearFrom=true&procurementStatus=&or_g_procurementInformations_type=true",
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
    # "HANDS": "https://hands.ehawaii.gov/hands/opportunities",
    "GoC": "https://canadabuys.canada.ca/en/tender-opportunities",
    "Scotland": "https://www.publiccontractsscotland.gov.uk/Search/Search_MainPage.aspx",
    "NRA": "https://www.nra.co.za/sanral-tenders/list/open-tenders",
    # "IADB": "https://projectprocurement.iadb.org/en/procurement-notices",
    "USAID": "https://www.usaid.gov/procurement-announcements",
    "AIIB": "https://www.aiib.org/en/opportunities/business/project-procurement/list.html",
    "EEAS": "https://www.eeas.europa.eu/eeas/tenders_en",
    "UNIDO": "https://www.unido.org/get-involved-procurement/procurement-opportunities",
    "CBK": "https://www.centralbank.go.ke/tenders/",
    "DevAID": "https://www.developmentaid.org/tenders/search?sectors=70",
    "Save the Children": "https://www.savethechildren.net/tenders",
    "TradeMarkAfrica": "https://www.trademarkafrica.com/procurement/",
    "IUCN": "https://iucn.org/procurement/currently-running-tenders",
    "KRA": "https://krc.co.ke/tenders/",
    "Enable": "https://www.enabel.be/fr/marches-publics/?in_category%5B%5D=all&in_country=all&is_status=0&_gl=1*arq3h4*_up*MQ..*_ga*NDM0NDQ0NTkxLjE2NzM1MzI2MzU.*_ga_9KW9PQQN9K*MTY3MzUzMjYzNC4xLjAuMTY3MzUzMjYzNC4wLjAuMA..#news",
    "Toronto": "https://www.toronto.ca/business-economy/doing-business-with-the-city/searching-bidding-on-city-contracts/toronto-bids-portal/#all",
    "India": "https://eprocure.gov.in/eprocure/app?component=%24DirectLink&page=FrontEndTendersByOrganisation&service=direct&sp=SaNQkFYrq2Ejxc9TMUtutTtS0Fec7wUuNy1YFXyqSerE%3D",
    "Arizona": "https://app.az.gov/page.aspx/en/rfp/request_browse_public",
    "Texas": "https://www.txsmartbuy.gov/esbd?page=1&keyword=software",
    "AU": "https://au.int/en/bids",
    "West Bengal": "https://www.wbsedcl.in/irj/go/km/docs/internet/new_website/TenderBids.html",
    "Gov-UK": "https://www.contractsfinder.service.gov.uk/Search/Results",
    "PPRA": "https://www.ppra.org.pk/dad_tenders.asp",
    "St. Vin": "https://procurement.gov.vc/eprocure/index.php/current-bids" ,
    "OSCE": "https://procurement.osce.org/tenders",
    "Bank of India": "https://bankofindia.co.in/tender",
    "Canara Bank": "https://canarabank.com/tenders",
    "E-Tender": "https://etenders.gov.in/eprocure/app?component=%24DirectLink&page=FrontEndTendersByOrganisation&service=direct&sp=SpUU0rj42FI3UoCfR2Ztdaw%3D%3D",
    "IICB": "https://iicb.res.in/tenders?status=active",
    "NUS": "https://www.nus.edu.sg/suppliers/business-opportunities",
    "Civic Info": "https://www.civicinfo.bc.ca/bids",
    "UNHCR Syria": "https://www.unhcr.org/sy/tender-announcements",   
    "Meghalaya" : "https://meghalaya.gov.in/tenders",
    "Uganda2": "https://egpuganda.go.ug/bid-notices",
    "H. Pradesh": "https://hptenders.gov.in/nicgep/app?component=%24DirectLink&page=FrontEndTendersByOrganisation&service=direct&sp=Su%2Bzb384sa%2FwA6xudXbBwXNS0Fec7wUuNy1YFXyqSerE%3D",
    "NGO-Proc": "https://procurement.ngojobsite.com/",
    "TB": "https://www.tenderboard.gov.bh/tenders/public%20tenders/",
    "KPPRA": "http://www.kppra.gov.pk/kppra/activetenders",
    "New India": "https://www.newindia.co.in/tender-notice",
    "Vic": "https://www.tenders.vic.gov.au/tender/search?preset=open",
    "AIIMS": "https://www.aiims.edu/index.php/en/tenders/aiims-tender",
    "Nepal": "https://bolpatra.gov.np/egp/searchOpportunity",
    "PSHD": "https://pshealthpunjab.gov.pk/Home/Tenders",
    "Mahapreit": "https://mahapreit.in/page/tender",
    "Prasarb": "https://prasarbharati.gov.in/pbtenders/",
    "Durban": "https://www.durban.gov.za/pages/business/procurement",
    "NCCF": "https://nccf-india.com/tenders/",
    "IOB": "https://www.iob.in/TenderDetails.aspx?Tendertype=Tender",
}

UNIVERSAL_LABELS = [
    "Title", "Description", "Date Posted", "Deadline", "Reference Number",
    "Category", "Location", "Language", "Contact", "Budget", "Type"
]

# Predefined tags for each website (can include both universal and specific labels)
PREDEFINED_TAGS = {
    # "https://tenders.go.ke/tenders": ["Tender No", "Description", "Category", "Deadline", "Location"],
    # "https://www.ungm.org/Public/Notice": ["Title", "Category", "Date Posted", "Deadline", "Type", "Location"],
    "https://www.iom.int/procurement-opportunities": ["Title", "Category", "Date Posted", "Deadline", "Type", "Location"],
    "https://www.ppda.mw/tenders": ["Title", "Category", "Date Posted", "Deadline", "Reference Number"],
    # "https://procurement-notices.undp.org/#:~:text=RFP/JSB-AC/2409/52%20Develop%20a%20national%20e-procurement": ["Title", "Ref No", "Date Posted", "Deadline", "Type", "Location"],
    "https://www.afdb.org/en/projects-and-operations/procurement#:~:text=Procurement%20procedures%20must%20offer%20equal%20opportunities%20to": ["Title", "Date Posted", "Type"],
    "https://www.kra.go.ke/tenders#:~:text=E%20-%20Procurement%20We%20are%20always%20working%20closely%20with%20our": ["Title", "Date Posted", "Deadline"],
    "https://esppra.co.sz/sppra/tender.php": ["Title", "Ref No", "Deadline", "Date Posted"],
    "https://www.publicprocurement.ng/#:~:text=ministry%20for%20local%20government%20and%20chieftaincy%20affairs,%20yobe": ["Description", "Date Added", "Deadline", "Type"],
    "https://gpp.ppda.go.ug/public/bid-invitations": ["Title", "Deadline", "Type"],
    "https://ec.europa.eu/info/funding-tenders/opportunities/portal/screen/opportunities/calls-for-tenders?keywords=software&isExactMatch=true&order=DESC&pageNumber=1&pageSize=50&sortBy=startDate": ["Title", "Deadline", "Type", "Status", "Date Posted"],
    "https://ssl.doas.state.ga.us/gpr/": ["Title", "Ref No", "Status", "Deadline", "Date Posted"],
    "https://www.eib.org/en/about/procurement/all/index.htm?q=&sortColumn=configuration.contentStart&sortDir=desc&pageNumber=0&itemPerPage=25&pageable=true&la=EN&deLa=EN&yearTo=&orYearTo=true&yearFrom=&orYearFrom=true&procurementStatus=&or_g_procurementInformations_type=true": ["Title", "Type", "Status", "Date Posted"],
    "https://www.un.org/Depts/ptd/eoi": ["Title", "Date Posted", "Deadline", "Reference Number"],
    "https://depedpines.com/procurement-notices/": ["Title", "Date Posted", "Deadline"],
    "https://www.gebiz.gov.sg/ptn/opportunity/BOListing.xhtml?origin=menu": ["Ref No", "Title", "Date Posted", "Deadline", "Cartegory", "Status"],
    "https://publicprocurement.govmu.org/publicprocurement/?page_id=720": ["Description", "Reference Number", "Deadline", "Cartegory"],
    "https://www.gov.bm/procurement-notices": ["Title", "Date Posted", "Deadline", "Ref No"],
    "https://www.caribank.org/work-with-us/procurement/general-procurement-notices": ["Title", "Cartegory", "Location"],
    "https://pcms2.gld.gov.hk/iprod/#/sta00305?lang-setting=en-US&results_pageNo=1": ["Description", "Deadline", "Ref No", "Cartegory"],
    "https://www.tenders.gov.au/atm": ["Description", "Deadline", "Cartegory", "Ref No"],
    "https://www.slcgmel.org/procurement-notices/": ["Title", "Date Posted", "Type"],
    "https://www.adb.org/projects/tenders/group/goods": ["Title", "Date Posted", "Deadline", "Type", "Ref No", "Status"],
    # "https://hands.ehawaii.gov/hands/opportunities": ["Title", "Location", "Deadline", "Cartegory", "Ref No", "Status", "Date Posted"],
    "https://canadabuys.canada.ca/en/tender-opportunities": ["Title", "Cartegory", "Deadline", "Date Posted"],
    "https://www.publiccontractsscotland.gov.uk/Search/Search_MainPage.aspx": ["Title", "Ref No", "Deadline", "Date Posted", "Type"],
    "https://www.nra.co.za/sanral-tenders/list/open-tenders": ["Description", "Ref No", "Deadline", "Location", "Type"],
    # "https://projectprocurement.iadb.org/en/procurement-notices": ["Title", "Ref No", "Deadline", "Type", "Location", "Date Posted"],
    "https://www.usaid.gov/procurement-announcements": ["Title", "Date Posted"],
    "https://www.aiib.org/en/opportunities/business/project-procurement/list.html": ["Cartegory", "Date Posted", "Title", "Type", "Location"],
    "https://www.eeas.europa.eu/eeas/tenders_en": ["Title", "Deadline", "Budget", "Type"],
    "https://www.unido.org/get-involved-procurement/procurement-opportunities": ["Title", "Deadline", "Type", "Location", "Ref No"],
    "https://www.centralbank.go.ke/tenders/": ["Title", "Date Posted", "Deadline", "Ref No", "Status"],
    "https://www.developmentaid.org/tenders/search?sectors=70": ["Title", "Deadline", "Type", "Location", "Status", "Budget", "Cartegory"],
    "https://www.savethechildren.net/tenders": ["Title", "Description", "Date Posted", "Location", "Deadline"],
    "https://www.trademarkafrica.com/procurement/": ["Title", "Ref No", "Deadline"],
    "https://iucn.org/procurement/currently-running-tenders": ["Title", "Deadline", "Location", "Budget"],
    "https://krc.co.ke/tenders/": ["Title", "Deadline", "Ref No", "Status"],
    "https://www.enabel.be/fr/marches-publics/?in_category%5B%5D=all&in_country=all&is_status=0&_gl=1*arq3h4*_up*MQ..*_ga*NDM0NDQ0NTkxLjE2NzM1MzI2MzU.*_ga_9KW9PQQN9K*MTY3MzUzMjYzNC4xLjAuMTY3MzUzMjYzNC4wLjAuMA..#news": ["Title", "Deadline", "Ref No", "Location"],
    "https://www.toronto.ca/business-economy/doing-business-with-the-city/searching-bidding-on-city-contracts/toronto-bids-portal/#all":  ["Title", "Deadline", "Ref No", "Date Posted", "Cartegory", "Type"],
    "https://eprocure.gov.in/eprocure/app?component=%24DirectLink&page=FrontEndTendersByOrganisation&service=direct&sp=SaNQkFYrq2Ejxc9TMUtutTtS0Fec7wUuNy1YFXyqSerE%3D":  ["Title", "Deadline", "Ref No", "Date Posted"],
    "https://app.az.gov/page.aspx/en/rfp/request_browse_public": ["Title", "Deadline", "Ref No", "Date Posted", "Cartegory", "Status"],
    "https://www.txsmartbuy.gov/esbd?page=1&keyword=software": ["Title", "Deadline", "Ref No", "Date Posted", ],
    "https://au.int/en/bids":  ["Title", "Deadline", "Ref No", "Type"],
    "https://www.wbsedcl.in/irj/go/km/docs/internet/new_website/TenderBids.html": ["Title", "Deadline", "Ref No", "Date Posted", "Budget"],
    "https://www.contractsfinder.service.gov.uk/Search/Results" : ["Title", "Deadline", "Budget", "Date Posted", "Cartegory", "Location"],
    "https://www.ppra.org.pk/dad_tenders.asp": ["Title", "Deadline", "Ref No", "Date Posted",],
    "https://procurement.gov.vc/eprocure/index.php/current-bids":  ["Description", "Ref No", "Deadline", "Type"],
    "https://procurement.osce.org/tenders": ["Title", "Deadline", "Date Posted",],
    "https://bankofindia.co.in/tender" : ["Title", "Deadline", "Ref No", "Date Posted"],
    "https://etenders.gov.in/eprocure/app?component=%24DirectLink&page=FrontEndTendersByOrganisation&service=direct&sp=SpUU0rj42FI3UoCfR2Ztdaw%3D%3D": ["Title", "Deadline", "Ref No", "Date Posted"],
    "https://iicb.res.in/tenders?status=active": ["Description", "Deadline", "Ref No", "Date Posted"],
    "https://www.nus.edu.sg/suppliers/business-opportunities": ["Description", "Deadline", "Ref No", "Date Posted", "Status"],
    "https://www.civicinfo.bc.ca/bids": ["Title", "Deadline", "Type", "Date Posted", "Location"],
    "https://www.unhcr.org/sy/tender-announcements": ["Title", "Date Posted"],
    "https://meghalaya.gov.in/tenders":  ["Title", "Deadline", "Date Posted"],
    "https://egpuganda.go.ug/bid-notices" : ["Title", "Deadline", "Date Posted", "Type", "Location", "Ref No"],
    "https://hptenders.gov.in/nicgep/app?component=%24DirectLink&page=FrontEndTendersByOrganisation&service=direct&sp=Su%2Bzb384sa%2FwA6xudXbBwXNS0Fec7wUuNy1YFXyqSerE%3D": ["Title", "Deadline", "Ref No", "Date Posted"],
    "https://procurement.ngojobsite.com/": ["Title", "Date Posted", "Type",],
    "https://www.tenderboard.gov.bh/tenders/public%20tenders/": ["Title", "Deadline", "Date Posted", "Type", "Cartegory"],
    "http://www.kppra.gov.pk/kppra/activetenders": ["Description", "Deadline", "Date Posted", "Ref No"],
    "https://www.newindia.co.in/tender-notice": ["Title", "Deadline", "Date Posted", "Location"],
    "https://www.tenders.vic.gov.au/tender/search?preset=open": ["Title", "Deadline", "Date Posted", "Type", "Status", "Ref No"],
    "https://www.aiims.edu/index.php/en/tenders/aiims-tender" : ["Title", "Deadline", "Date Posted", "Cartegory"],
    "https://bolpatra.gov.np/egp/searchOpportunity": ["Title", "Deadline", "Date Posted", "Type", "Ref No"],
    "https://pshealthpunjab.gov.pk/Home/Tenders": ["Title",  "Date Posted",],
    "https://mahapreit.in/page/tender": ["Title", "Cartegory", "Date Posted", "Type", "Ref No"],
    "https://www.durban.gov.za/pages/business/procurement": ["Title", "Deadline", "Cartegory", "Type", "Ref No"],
    "https://nccf-india.com/tenders/": ["Title", "Date Posted"],
    "https://www.iob.in/TenderDetails.aspx?Tendertype=Tender":  ["Description", "Deadline", "Date Posted",]
    
}

def generate_unique_folder_name(url):
    timestamp = datetime.now().strftime('%Y_%m_%d__%H_%M_%S')
    parsed_url = urlparse(url)
    domain = parsed_url.netloc or parsed_url.path.split('/')[0]
    domain = re.sub(r'^www\.', '', domain)
    clean_domain = re.sub(r'\W+', '_', domain)
    return f"{clean_domain}_{timestamp}"

def scrape_multiple_urls(urls, fields, selected_model):
    output_folder = os.path.join('output', generate_unique_folder_name(urls[0]))
    os.makedirs(output_folder, exist_ok=True)
    
    total_input_tokens = 0
    total_output_tokens = 0
    total_cost = 0
    all_data = []
    first_url_markdown = None
    
    start_time = time.time()  # Record the start time
    
    for i, url in enumerate(urls, start=1):
        raw_html = fetch_html_selenium(url)
        markdown = html_to_markdown_with_readability(raw_html)
        if i == 1:
            first_url_markdown = markdown
        
        input_tokens, output_tokens, cost, formatted_data = scrape_url(url, fields, selected_model, output_folder, i, markdown)
        total_input_tokens += input_tokens
        total_output_tokens += output_tokens
        total_cost += cost
        all_data.append(formatted_data)
    
    end_time = time.time()  # Record the end time
    scraping_time = end_time - start_time  # Calculate the total time taken
    
    return output_folder, total_input_tokens, total_output_tokens, total_cost, all_data, first_url_markdown, scraping_time


def perform_scrape():
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    raw_html = fetch_html_selenium(url_input)
    markdown = html_to_markdown_with_readability(raw_html)
    save_raw_data(markdown, timestamp)
    
    pagination_info = None
    if use_pagination:
        pagination_data, token_counts, pagination_price = detect_pagination_elements(
            url_input, pagination_details, model_selection, markdown
        )
        pagination_info = {
            "page_urls": pagination_data.page_urls,
            "token_counts": token_counts,
            "price": pagination_price
        }
    
    input_tokens = output_tokens = total_cost = 0
    
    if show_tags:
        DynamicListingModel = create_dynamic_listing_model(selected_labels)
        DynamicListingsContainer = create_listings_container_model(DynamicListingModel)
        formatted_data, tokens_count = format_data(
            markdown, DynamicListingsContainer, DynamicListingModel, model_selection
        )
        input_tokens, output_tokens, total_cost = calculate_price(tokens_count, model=model_selection)
        df = save_formatted_data(formatted_data, timestamp)
    else:
        formatted_data = None
        df = None
    
    return df, formatted_data, markdown, input_tokens, output_tokens, total_cost, timestamp, pagination_info

# Sidebar components
st.sidebar.title("Web Scraper Settings")

# Dropdown to select the website by name
selected_website_name = st.sidebar.selectbox("Select Website", options=list(WEBSITE_URLS.keys()))

# Get the corresponding URL for the selected website name
selected_website_url = WEBSITE_URLS[selected_website_name]

# Combine universal labels with predefined tags for the selected website
combined_labels = list(set(UNIVERSAL_LABELS + PREDEFINED_TAGS.get(selected_website_url, [])))

model_selection = st.sidebar.selectbox("Select Model", options=list(PRICING.keys()), index=0)
url_input = st.sidebar.text_input("Enter URL", value=selected_website_url)

# Dropdown for selecting labels
selected_labels = st.sidebar.multiselect(
    "Select Fields to Extract:",
    options=combined_labels,
    default=PREDEFINED_TAGS.get(selected_website_url, UNIVERSAL_LABELS[:5])
)

# Add toggle to show/hide tags field
show_tags = st.sidebar.checkbox("Enable Scraping")

st.sidebar.markdown("---")
# Add pagination toggle and input
use_pagination = st.sidebar.checkbox("Enable Pagination")
pagination_details = None
if use_pagination:
    pagination_details = st.sidebar.text_input("Enter Pagination Details (optional)", 
        help="Describe how to navigate through pages (e.g., 'Next' button class, URL pattern)")

st.sidebar.markdown("---")

def save_scraped_data_as_json(data, output_folder):
    """
    Saves the scraped data as a JSON file in the specified output folder.
    
    Args:
        data (dict or list): The scraped data to save in JSON format.
        output_folder (str): The path to the folder where the JSON file will be saved.
    
    Returns:
        str: The path to the saved JSON file.
    """
    # Ensure the output folder exists
    os.makedirs(output_folder, exist_ok=True)
    
    # Create a timestamped filename
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    json_filename = f"scraped_data_{timestamp}.json"
    json_filepath = os.path.join(output_folder, json_filename)
    
    # Save the data to a JSON file
    try:
        with open(json_filepath, 'w', encoding='utf-8') as json_file:
            json.dump(data, json_file, ensure_ascii=False, indent=4)
        return json_filepath
    except Exception as e:
        raise RuntimeError(f"Failed to save scraped data to JSON: {e}")


if st.sidebar.button("Scrape"):
    with st.spinner('Please wait... Data is being scraped and pushed to the database.'):
        try:
            urls = url_input.split()
            field_list = selected_labels
            # Perform the scraping operation
            output_folder, total_input_tokens, total_output_tokens, total_cost, all_data, first_url_markdown, scraping_time = scrape_multiple_urls(urls, field_list, model_selection)
            
            # Handle pagination if enabled and there is only one URL
            pagination_info = None
            if use_pagination and len(urls) == 1:
                try:
                    pagination_result = detect_pagination_elements(
                        urls[0], pagination_details, model_selection, first_url_markdown
                    )
                    
                    if pagination_result is not None:
                        pagination_data, token_counts, pagination_price = pagination_result
                        page_urls = pagination_data.page_urls if isinstance(pagination_data, PaginationData) else pagination_data.get("page_urls", [])
                        
                        pagination_info = {
                            "page_urls": page_urls,
                            "token_counts": token_counts,
                            "price": pagination_price
                        }
                    else:
                        st.warning("Pagination detection returned None. No pagination information available.")
                except Exception as e:
                    st.error(f"An error occurred during pagination detection: {e}")
                    pagination_info = {
                        "page_urls": [],
                        "token_counts": {"input_tokens": 0, "output_tokens": 0},
                        "price": 0.0
                    }
            
            # Push scraped data to the database
            try:
                # Convert the scraped data to JSON and save it to a file
                formatted_json_path = save_scraped_data_as_json(all_data, output_folder)
                
                # Push the JSON data to PostgreSQL
                push_json_to_db(formatted_json_path, table_name='scraped_data', website_name=selected_website_name, website_url=selected_website_url)
                
                # Push website information to PostgreSQL
                push_website_info_to_db(
                    website_url=selected_website_url,
                    website_name=selected_website_name,
                    labels=selected_labels,
                    table_name='website_info'
                )
                st.success("Data and website information successfully saved to the database.")
            except Exception as e:
                st.error(f"An error occurred during database insertion: {e}")
                logger.error(f"Database insertion error: {e}", exc_info=True)
            
            # Update session state with all results, including pagination information
            st.session_state['results'] = (all_data, first_url_markdown, total_input_tokens, total_output_tokens, total_cost, output_folder, pagination_info, scraping_time)
            st.session_state['perform_scrape'] = True

        except Exception as e:
            st.error(f"An error occurred during scraping or database insertion: {e}")
            logger.error(f"Scraping or database error: {e}", exc_info=True)
            st.session_state['perform_scrape'] = False

# if st.button("Push to Database"):
#     if 'results' in st.session_state and st.session_state['results']:
#         _, _, _, _, _, output_folder, _, _ = st.session_state['results']
#         with st.spinner('Pushing data to database...'):
#             # Pass the selected website name to the function
#             # success, message = push_data_to_db(output_folder, selected_website_name)
#             # if success:
#             #     st.success(message)
#             #     logger.info(message)
#             # else:
#             #     st.error(message)
#             #     logger.error(message)
            
#             # Display logs
#             st.subheader("Operation Logs")
#             log_output = st.empty()
#             with open('app.log', 'r') as log_file:
#                 logs = log_file.read()
#                 log_output.text_area("Logs", logs, height=300)
#     else:
#         st.warning("No scraping results available. Please scrape data first.")
#         logger.warning("Attempted to push to database without scraping results")

if st.session_state['perform_scrape']:
    st.success("Scraping completed.")
    
    # Display results if they exist in session state
    if st.session_state['results']:
        all_data, first_url_markdown, input_tokens, output_tokens, total_cost, output_folder, pagination_info, scraping_time = st.session_state['results']
        
        if all_data and show_tags:
            st.sidebar.markdown("---")
            st.sidebar.markdown("### Scraping Details")
            st.sidebar.markdown(f"**Time Taken:** {scraping_time:.2f} seconds")
            st.sidebar.markdown("#### Token Usage")
            st.sidebar.markdown(f"*Input Tokens:* {input_tokens}")
            st.sidebar.markdown(f"*Output Tokens:* {output_tokens}")
            st.sidebar.markdown(f"**Total Cost:** :green-background[**${total_cost:.4f}**]")

            st.subheader("Scraped/Parsed Data")
            for i, data in enumerate(all_data, start=1):
                st.write(f"Data from URL {i}:")

                if isinstance(data, str):
                    try:
                        data = json.loads(data)
                    except json.JSONDecodeError:
                        st.error(f"Failed to parse data as JSON for URL {i}")
                        continue
                
                if isinstance(data, dict):
                    if 'listings' in data and isinstance(data['listings'], list):
                        df = pd.DataFrame(data['listings'])
                    else:
                        df = pd.DataFrame([data])
                elif hasattr(data, 'listings') and isinstance(data.listings, list):
                    listings = [item.dict() for item in data.listings]
                    df = pd.DataFrame(listings)
                else:
                    st.error(f"Unexpected data format for URL {i}")
                    continue
                
                st.dataframe(df, use_container_width=True)

            st.subheader("Download Options")
            col1, col2 = st.columns(2)
            with col1:
                json_data = json.dumps(all_data, default=lambda o: o.dict() if hasattr(o, 'dict') else str(o), indent=4)
                st.download_button(
                    "Download JSON",
                    data=json_data,
                    file_name="scraped_data.json"
                )
            with col2:
                all_listings = []
                for data in all_data:
                    if isinstance(data, str):
                        try:
                            data = json.loads(data)
                        except json.JSONDecodeError:
                            continue
                    if isinstance(data, dict) and 'listings' in data:
                        all_listings.extend(data['listings'])
                    elif hasattr(data, 'listings'):
                        all_listings.extend([item.dict() for item in data.listings])
                    else:
                        all_listings.append(data)
                
                combined_df = pd.DataFrame(all_listings)
                st.download_button(
                    "Download CSV",
                    data=combined_df.to_csv(index=False),
                    file_name="scraped_data.csv"
                )

            st.success(f"Scraping completed. Results saved in {output_folder}")

        if pagination_info and use_pagination:
            st.sidebar.markdown("---")
            st.sidebar.markdown("### Pagination Details")
            st.sidebar.markdown(f"**Number of Page URLs:** {len(pagination_info['page_urls'])}")
            st.sidebar.markdown("#### Pagination Token Usage")
            st.sidebar.markdown(f"*Input Tokens:* {pagination_info['token_counts']['input_tokens']}")
            st.sidebar.markdown(f"*Output Tokens:* {pagination_info['token_counts']['output_tokens']}")
            st.sidebar.markdown(f"**Pagination Cost:** :red-background[**${pagination_info['price']:.4f}**]")

            st.markdown("---")
            st.subheader("Pagination Information")
            pagination_df = pd.DataFrame(pagination_info["page_urls"], columns=["Page URLs"])
            
            st.dataframe(
                pagination_df,
                column_config={
                    "Page URLs": st.column_config.LinkColumn("Page URLs")
                }, use_container_width=True
            )

            col1, col2 = st.columns(2)
            with col1:
                st.download_button(
                    "Download Pagination JSON", 
                    data=json.dumps(pagination_info["page_urls"], indent=4), 
                    file_name=f"pagination_urls.json"
                )
            with col2:
                st.download_button(
                    "Download Pagination CSV", 
                    data=pagination_df.to_csv(index=False), 
                    file_name=f"pagination_urls.csv"
                )

        if all_data and pagination_info and show_tags and use_pagination:
            st.markdown("---")
            total_input_tokens = input_tokens + pagination_info['token_counts']['input_tokens']
            total_output_tokens = output_tokens + pagination_info['token_counts']['output_tokens']
            total_combined_cost = total_cost + pagination_info['price']
            st.markdown("### Total Counts and Cost (Including Pagination)")
            st.markdown(f"**Total Input Tokens:** {total_input_tokens}")
            st.markdown(f"**Total Output Tokens:** {total_output_tokens}")
            st.markdown(f"**Total Combined Cost:** :green[**${total_combined_cost:.4f}**]")

if st.sidebar.button("Clear Results"):
    st.session_state['results'] = None
    st.session_state['perform_scrape'] = False
    st.rerun()
