# Universal Web Scraper 🦑

A sophisticated web scraping tool designed to extract procurement opportunities from various government and international organization websites, with a focus on IT and software-related tenders.

## Features

- Multi-model support (GPT-4, Gemini, Groq Llama)
- Pagination detection and handling
- Database integration (PostgreSQL)
- Customizable field extraction
- Support for multiple website templates
- Token usage tracking and cost calculation
- Export to JSON and CSV formats
- User-friendly Streamlit interface

## Prerequisites

### Software Requirements
- Python 3.8+
- PostgreSQL
- Chrome/Chromium browser
- ChromeDriver

### API Keys
The following API keys need to be set in your `.env` file:
```env
OPENAI_API_KEY=your_openai_key
GOOGLE_API_KEY=your_google_key
GROQ_API_KEY=your_groq_key

# PostgreSQL Configuration
POSTGRES_HOST=your_host
POSTGRES_PORT=your_port
POSTGRES_DB=your_database
POSTGRES_USER=your_username
POSTGRES_PASSWORD=your_password
```

## Installation

1. Clone the repository:
```bash
git clone <https://github.com/k-kipyego/UniversalScraper.git>
cd universal-web-scraper
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set up PostgreSQL database and update environment variables.

5. Ensure ChromeDriver is installed and in your system PATH.

## Project Structure

- `assets.py`: Configuration variables and constants
- `database_push.py`: Database interaction functions
- `db_connection.py`: Database connection management
- `pagination_detector.py`: Pagination handling logic
- `scraper.py`: Core scraping functionality
- `streamlit_app.py`: Web interface

## Usage

1. Start the Streamlit application:
```bash
streamlit run streamlit_app.py
```

2. In the web interface:
   - Select a website from the dropdown or enter a custom URL
   - Choose the AI model to use
   - Select fields to extract
   - Enable/disable pagination if needed
   - Click "Scrape" to start the process

3. View and download results in JSON or CSV format

## Supported Websites

The tool comes pre-configured with templates for numerous procurement websites, including:
- UNDP
- AFDB
- UN
- ADB
- USAID
- And many more (see `WEBSITE_URLS` in `streamlit_app.py`)

## Data Models

### Scraped Data Structure
```python
{
    "listings": [
        {
            "Title": str,
            "Description": str,
            "Date Posted": str,
            "Deadline": str,
            "Reference Number": str,
            "Category": str,
            "Location": str,
            "Language": str,
            "Contact": str,
            "Budget": str,
            "Type": str,
            "direct_url": str
        }
    ]
}
```

## Database Schema

### Main Tables
1. `scraped_data`: Stores the raw scraped data
2. `structured_scraped_data`: Stores processed procurement notices
3. `website_info`: Stores website configurations and metadata

## Error Handling

The application includes comprehensive error handling and logging:
- Failed scraping attempts are logged
- Database connection issues are reported
- API errors are caught and displayed
- Token usage is monitored and reported

## Performance Considerations

- Uses Selenium with Chrome in headless mode
- Implements random delays and user agent rotation
- Supports batch processing for multiple URLs
- Includes token usage optimization

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request


## Support

For issues and feature requests, please create an issue in the repository.