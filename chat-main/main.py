from dns.e164 import query
from fastapi.middleware.cors import CORSMiddleware
import logging
from openai import OpenAI
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pymongo import MongoClient
from certifi import where
import os

from settings.utils import json_cleaner, news_summarizer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Financial_Bot",
    description="APIs for Financial Bot",
    version="1.0.0",
    openapi_url="/openapi.json",
    docs_url="/"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

client = OpenAI()
mongo_client = MongoClient(os.environ["MONGO_URI"], tlsCAFile=where())
db = mongo_client["informatica_ai"]
user_preferences_db = db["user_preferences"]
all_news_db = db["all_news"]

@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.post("/financial_bot/v1/chat")
async def chat(message:str, history: list[str] =[], stock_name:str=None, current_news:str=None):

    try:
        if message is None:
            return JSONResponse(content={"message": "Message is required"}, status_code=400)

        # if current_news is None:
        #     current_news = news_summarizer(query=message, stock_name=stock_name)
        history_str = "".join(history[::-1])

        if stock_name is None:
            stock_name = "All stock information, no specific stock mentioned"

        completion = client.chat.completions.create(
            model='o3-mini',
            messages=[
                {'role': 'system', 'content': 'You are a financial advisor, who is provided with information from multiple news sources & Vector Database around the message of User. Analyze the information snippets, and give a clear & crisp answer with reasons.'},
                {'role': 'user', 'content': f'Current User Message {message}'},
                {'role': 'user', 'content': f'User is particularly interested in Stock - {stock_name}'},
                {'role': 'user', 'content': f'Conversation History so far - {history_str}'},
                {'role': 'system', 'content': f'Latest Content from Yahoo Finance {current_news}'}

            ]
        )
        response_content = json_cleaner(completion.choices[0].message.content)
        history.append(str({message:response_content}))
        return JSONResponse(content={"message": response_content, "history": history, "stock_name": stock_name, "current_news": current_news}, status_code=200)

    except Exception as e:
        logger.error(f"Error in processing request chat: {str(e)}")
        return JSONResponse(content={"message": "Internal Server Error"}, status_code=500)

@app.post("/financial_bot/v1/add_user_preference")
async def add_user_preference(email_id:str, preference:str):
    try:
        if email_id is None or preference is None:
            return JSONResponse(content={"message": "User ID and Preference are required"}, status_code=400)
        else:
            if email_id in user_preferences_db.distinct("user_id"):
                user_preferences_db.update_one({"user_id": email_id}, {"$set": {"preference": preference}})
            else:
                user_preferences_db.insert_one({"user_id": email_id, "preference": preference})
            return JSONResponse(content={"message": "User preference added successfully"}, status_code=200)

    except Exception as e:
        logger.error(f"Error in processing request add_user_preference: {str(e)}")
        return JSONResponse(content={"message": "Internal Server Error"}, status_code=500)

@app.get("/financial_bot/v1/get_user_preference")
async def get_user_preference(email_id:str):
    try:
        if email_id is None:
            return JSONResponse(content={"message": "User ID is required"}, status_code=400)
        else:
            if email_id in user_preferences_db.distinct("user_id"):
                preference = user_preferences_db.find_one({"user_id": email_id}, {"_id": 0, "preference": 1})
                return JSONResponse(content={"message": preference}, status_code=200)
            else:
                return JSONResponse(content={"message": "User preference not found"}, status_code=404)

    except Exception as e:
        logger.error(f"Error in processing request get_user_preference: {str(e)}")
        return JSONResponse(content={"message": "Internal Server Error"}, status_code=500)

@app.get("/financial_bot/v1/get_latest_news_snippets")
async def get_latest_news_snippets(topic: str = None):
    try:
        pipeline = [
            {"$sort": {"_id": -1}},
            {"$limit": 10},
            {"$project": {"_id": 0}},
            {"$replaceRoot": {"newRoot": {"$mergeObjects": [
                {"news_sources": {"$objectToArray": "$$ROOT"}},
                {"_id": "$_id"}
            ]}}},
            {"$unwind": "$news_sources"},
            {"$project": {"source": "$news_sources.k", "articles": "$news_sources.v"}},
            {"$unwind": "$articles"},
            {"$match": {"articles.topic": topic}} if topic else {"$match": {}},
            {"$group": {"_id": "$source", "articles": {"$push": "$articles"}}},
            {"$project": {"_id": 0, "source": "$_id", "articles": 1}}
        ]

        news_snippets = list(all_news_db.aggregate(pipeline))

        return JSONResponse(content={"message": news_snippets}, status_code=200)

    except Exception as e:
        logger.error(f"Error in processing request get_latest_news_snippets: {str(e)}")
        return JSONResponse(content={"message": "Internal Server Error ,"}, status_code=500)


#
#
# if __name__ == "__main__":
#     import uvicorn
#
#     uvicorn.run(app, host="127.0.0.1", port=8000)
# command to run on server gunicorn -w 4 -k uvicorn.workers.UvicornWorker main:app






