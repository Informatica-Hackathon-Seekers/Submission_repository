import logging
import json
from firecrawl import FirecrawlApp
import os
from openai import OpenAI
from langchain_milvus import Milvus
from langchain_openai import OpenAIEmbeddings


logger = logging.getLogger(__name__)
firecrawl_key = os.environ["FIRECRAWL_KEY"]
crawl_app = FirecrawlApp(api_key=firecrawl_key)
openai_client = OpenAI()
ZILLIZ_URL = "https://in03-e5bab4e640f79fb.serverless.gcp-us-west1.cloud.zilliz.com"
ZILLIZ_TOKEN = os.environ.get("ZILLIZ_TOKEN")
vector_store = Milvus(embedding_function=OpenAIEmbeddings(), connection_args={"uri": ZILLIZ_URL, "token": ZILLIZ_TOKEN}, auto_id=True, collection_name="informatica_all_news")

def json_cleaner(data):
    """
    Cleans data to make it JSON-compatible by removing unnecessary whitespaces and ensuring proper quotation marks.
    :param data: Input data as a string
    :return: JSON-compatible data or original string if parsing fails
    """
    try:
        # Convert data to a string if not already
        data = str(data)

        # Remove extra newlines and whitespace
        data = " ".join(data.split())

        # Ensure JSON-friendly quotes
        data = data.replace("\n", " ")
        data = data.replace("\r", " ")
        data = data.replace("  ", " ")
        data = data.replace("\\", "")

        # Attempt to parse and load as JSON
        return json.loads(data)
    except json.JSONDecodeError as e:
        logger.error(f"JSON decoding error: {str(e)}")
        return data  # Return the original data if parsing fails
    except Exception as e:
        logger.error(f"Error in cleaning data: {str(e)}")
        return data

def news_summarizer(query:str,stock_name:str=None):
    news = get_latest_news_yahoo(stock_name)
    data = get_data_from_milvus(query)

    response = openai_client.chat.completions.create(
        model='gpt-4o',
        messages=[
            {'role': 'system', 'content': f'You are a news extractor & summarizer, who is provided with information from multiple news sources & Database, extract relevant information around {query} from news, and summarize in 1-2 paragraphs'},
            {'role': 'user', 'content': f'News Headlines {news}'},
            {'role':'user','content': f'Data from Database {data}'}
        ]
    )
    return json_cleaner(response.choices[0].message.content)

def get_latest_news_yahoo(stock_name:str=None):
    if stock_name is None:
        url = "https://finance.yahoo.com/"
    else:
        url = f"https://finance.yahoo.com/quote/{stock_name}"

    try:
        scrape_result = crawl_app.scrape_url(url, params={'formats': ['markdown']})
        return str(scrape_result)
    except Exception as e:
        logger.error(f"Error in getting latest news yahoo: {str(e)}")
        return ""

def get_data_from_milvus(query:str=None):
    try:
        if query is None:
            return ""

        data = vector_store.similarity_search(query, top_k=5)
        return str(data)
    except Exception as e:
        logger.error(f"Error in getting data from Milvus: {str(e)}")
        return ""


