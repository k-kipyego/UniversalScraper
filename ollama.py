import os
import requests

# Directory containing your Markdown files
MD_DIRECTORY = 'C:/ScrapeMaster/output'  # Update this path accordingly
# Output directory for extracted text files
OUTPUT_DIRECTORY = 'C:/ScrapeMaster/extractedData'  # Update this path accordingly
# Ollama model details
OLLAMA_MODEL = 'reader-lm:latest'
OLLAMA_URL = 'http://localhost:11434/api/generate'  # Adjust if your Ollama instance uses another port

# Function to read markdown files and extract specific details using Ollama
def extract_details_from_markdown(md_file_path):
    try:
        # Read the Markdown file
        with open(md_file_path, 'r', encoding='utf-8') as md_file:
            markdown_content = md_file.read()
        
        # Prepare the request payload for Ollama with a specific prompt to extract details
        prompt = f"""
        The following is a Markdown file. Extract the following details:
        - Procurement Title
        - Description
        - Posted Date
        - Due Date
        - Language
        - Country
        - Source Link
        The Markdown content is below:
        {markdown_content}
        """
        
        payload = {
            "model": OLLAMA_MODEL,
            "prompt": prompt,
            "system": "Extract the requested details from the markdown."
        }

        # Send the request to Ollama
        response = requests.post(OLLAMA_URL, json=payload)
        response.raise_for_status()

        # Capture and return the plain text response from Ollama
        return response.text

    except Exception as e:
        print(f"Error extracting details from file {md_file_path}: {e}")
        return None

# Function to process all Markdown files in a directory
def process_markdown_files(md_directory):
    if not os.path.exists(OUTPUT_DIRECTORY):
        os.makedirs(OUTPUT_DIRECTORY)

    for root, dirs, files in os.walk(md_directory):
        for file in files:
            if file.endswith('.md'):
                md_file_path = os.path.join(root, file)
                print(f"Processing {md_file_path}...")

                # Extract details from markdown
                extracted_text = extract_details_from_markdown(md_file_path)

                if extracted_text:
                    # Save the extracted details as a plain text file
                    output_file_path = os.path.join(OUTPUT_DIRECTORY, f"{os.path.splitext(file)[0]}_extracted.txt")
                    with open(output_file_path, 'w', encoding='utf-8') as text_file:
                        text_file.write(extracted_text)

                    print(f"Extracted details from {md_file_path} and saved to {output_file_path}")

if __name__ == "__main__":
    process_markdown_files(MD_DIRECTORY)
