import os
import time
from azure.storage.queue import QueueServiceClient, QueueClient, QueueMessage, BinaryBase64DecodePolicy, BinaryBase64EncodePolicy
import json
from pymongo import MongoClient
from certifi import where
from openai import OpenAI
from langchain_milvus import Milvus
from langchain_openai import OpenAIEmbeddings


azure_queue_name = "raw-to-informatica-queue"
azure_queue_url = "https://rawtoinformatica.queue.core.windows.net/raw-to-informatica-queue"
azure_connection_string = os.environ["AZURE_CONNECTION_STRING"]
ZILLIZ_URL = "https://in03-e5bab4e640f79fb.serverless.gcp-us-west1.cloud.zilliz.com"
ZILLIZ_TOKEN = os.environ.get("ZILLIZ_TOKEN")
vector_store = Milvus(embedding_function=OpenAIEmbeddings(), connection_args={"uri": ZILLIZ_URL, "token": ZILLIZ_TOKEN}, auto_id=True, collection_name="informatica_all_news")


openai_client = OpenAI()
mongo_client = MongoClient(os.environ["MONGO_URI"], tlsCAFile=where())
db = mongo_client["informatica_ai"]
all_news_db = db["all_news"]

queue_client = QueueClient.from_connection_string(azure_connection_string, azure_queue_name, message_encode_policy=BinaryBase64EncodePolicy(), message_decode_policy=BinaryBase64DecodePolicy())

def save_news_message_to_mongo(data):
    try:
        all_news_db.insert_one(data)
        print("Data saved to MongoDB")
    except Exception as e:
        print(f"Error saving data to MongoDB: {e}")

def save_news_vector_to_zilliz(data):
    try:
        vector_store.add_texts(str(data))
        print("Data saved to Zilliz")
    except Exception as e:
        print(f"Error saving data to Zilliz: {e}")


def clean_json(data):
    try:
        data = str(data)
        data = " ".join(data.split())
        data = data.replace("\n", " ")
        data = data.replace("  ", " ")
        data = data.replace("\\", "")
        data = data.replace("```", "")
        data = data.replace("```python", "")
        data = data.replace("```json", "")
        data = data.replace("```yaml", "")
        data = data.replace("json", "")
        data = data.replace("https://","")
        data = data.replace("https:","")
        return json.loads(data)
    except json.JSONDecodeError as e:
        print(f"JSON decoding error: {str(e)}")
        return data
    except Exception as e:
        print(f"Error in cleaning data: {str(e)}")
        return data

def create_news_summaries(data):
    print("Creating news summaries")
    completion = openai_client.chat.completions.create(
        model = 'gpt-4o',
        messages=[{"role":"system","content":"You are a news summarizer, consider the raw json below, and give me response in a dictionary format, without any special chracters."},
                  {"role":"system","content":"Required Format Strictly JSON - {'Yahoo News':['title':'title of the news','summary':'summary of the news/ Headline completed for proper grammer','link':'link to the news','topic':'Stocks or market impacted out of [Minerals, Technology, Real Estate, Politics, Healthcare, Energy, Consumer Goods, Financial Services, Telecommunications, Utilities, Electronics]'],'Bloomberg'....}"},
                  {"role":"system","content":"use single quote for apostrophes , and double quote for key value pairs of json, and comma to separate the news sources, news source is key, its value is an array of dictionaries, each dictionary is a news item, with title, summary, link, topic"},
                  {"role":"user","content":str(data)}])
    return completion.choices[0].message.content

def listen_to_queue():

    while True:
        message = queue_client.receive_message()
        if message:
            print("Message received: {}".format(message.content))
            queue_client.delete_message(message)
            #base64 decode
            # message = message.content.decode('utf-8')
            # data = json.loads(message.content)
            data = message.content
            news_summaries = create_news_summaries(data)
            news_summaries = clean_json(news_summaries)
            print(news_summaries)
            save_news_message_to_mongo(news_summaries)
            save_news_vector_to_zilliz(news_summaries)
        else:
            print("No messages in queue")

    #wait 4 hours
        time.sleep(14400)

if __name__ == "__main__":
    listen_to_queue()