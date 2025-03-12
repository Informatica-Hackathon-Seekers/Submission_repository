import os
import time
from azure.storage.queue import QueueServiceClient, QueueClient, QueueMessage, BinaryBase64DecodePolicy, BinaryBase64EncodePolicy
from firecrawl import FirecrawlApp
from google.cloud import pubsub_v1
import json
import ast
import re
import base64



firecrawl_key = os.environ["FIRECRAWL_KEY"]
azure_queue_name = "raw-to-informatica-queue"
azure_queue_url = "https://rawtoinformatica.queue.core.windows.net/raw-to-informatica-queue"
azure_connection_string = os.environ["AZURE_CONNECTION_STRING"]

project_id = os.environ["PROJECT_ID"]
topic_id = os.environ["PUB_TOPIC_ID"]
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)
# print("Publishing message to topic: {}".format(topic_path))

crawl_app = FirecrawlApp(api_key=firecrawl_key)
queue_client = QueueClient.from_connection_string(azure_connection_string, azure_queue_name, message_encode_policy=BinaryBase64EncodePolicy(), message_decode_policy=BinaryBase64DecodePolicy())


#Function to extract the news
def extract_news(url):
    #Basic Crawl for MVP
    scrape_result = crawl_app.scrape_url(url, params={'formats': ['markdown']})
    return str(scrape_result)

#push raw data to kafka
def push_to_google_pub_sub(data):
    data = str(data).encode('utf-8')
    future = publisher.publish(topic_path, data)
    print(future.result())

def push_to_azure_queue(data):
    #ENCODE base 64
    data = data.encode('utf-8')
    queue_client.send_message(data)
    print("Message sent to Azure Queue")


def format_json(data):
    """
    Robustly convert Yahoo Finance JSON data to a properly formatted JSON string.

    Args:
        data: Input data to be formatted (dict, str, etc.)

    Returns:
        str: Properly formatted JSON string
    """
    try:
        # Handle different input types
        if isinstance(data, str):
            # Remove newline characters and specific markers
            data = data.replace('\n', '').replace('\\_ns', '')

            # Try multiple parsing strategies
            try:
                # First, try direct JSON parsing
                parsed_data = json.loads(data)
            except json.JSONDecodeError:
                try:
                    # If JSON parsing fails, try literal evaluation
                    parsed_data = ast.literal_eval(data)
                except (SyntaxError, ValueError):
                    # Last resort: attempt to clean and parse
                    try:
                        # Remove leading/trailing braces if misplaced
                        data = data.strip('{}')

                        # Add quotes around keys if missing
                        data = re.sub(r'(\w+):', r'"\1":', data)
                        parsed_data = json.loads('{' + data + '}')
                    except Exception:
                        return str(data)
        else:
            # If not a string, assume it's already a dict or can be directly converted
            parsed_data = data

        # Special handling for Yahoo Finance nested JSON
        if isinstance(parsed_data, dict) and 'markdown' in parsed_data:
            # Extract and parse the nested markdown/JSON content
            try:
                nested_content = json.loads(parsed_data['markdown'])
                parsed_data['markdown_parsed'] = nested_content
            except Exception:
                pass

        # Convert to JSON string with enhanced formatting
        return json.dumps(parsed_data,
                          indent=2,
                          ensure_ascii=False,
                          sort_keys=True)

    except Exception as e:
        # Comprehensive error handling
        return f"Error formatting JSON: {str(e)}"



if __name__ == "__main__":
    us_finance_new_sources = ["https://www.finance.yahoo.com","https://www.google.com/finance/?hl=en"]
    data_in={}
    while True:
        for url in us_finance_new_sources:
            print(url)
            data = extract_news(url)
            data_in.update({
                "url": url,
                "data": data})


            #Push to Azure Queue
        push_to_azure_queue(json.dumps(data_in))
        #Push to Google Pub/Sub
        push_to_google_pub_sub(json.dumps(data_in))

        #wait 6 hours
        time.sleep(21600)


