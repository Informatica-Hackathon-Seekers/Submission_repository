import copy
import json
import uuid
import urllib3
from certifi import where
from pymongo import MongoClient
import os
from bs4 import BeautifulSoup
from collections import defaultdict
import time


mongo_client = MongoClient(os.environ["MONGO_URI"], tlsCAFile=where())
novu_key = os.environ['NOVU_KEY']
db = mongo_client["informatica_ai"]
all_news_db = db["all_news"]
user_preferences_db = db["user_preferences"]
template_path = os.path.join(os.path.dirname(__file__), "email_template.html")
template = open(template_path).read()
soup = BeautifulSoup(template, "html.parser")

article_template = soup.find('div', class_='columns')

if not article_template:
    raise ValueError("Article template with class 'columns' not found in the HTML template.")

# Split HTML into start and end parts
article_str = str(article_template)
html_start = str(soup).split(article_str, 1)[0].replace('\n', '')
html_end = str(soup).split(article_str, 1)[1].replace('\n', '')

def prepare_news_letter(news):
    html = html_start
    for news_item in news:
        article = copy.deepcopy(article_template)  # Deep copy to avoid modifying the original
        # Set article details
        if article.find('h1'):
            article.find('h1').string = news_item["title"]
        if article.find('p'):
            article.find('p').string = news_item["summary"]
        if article.find('a'):
            article.find('a')['href'] = news_item["link"]
            article.find('a').string = f"Read More on {news_item['source']}"

        # Handle multiple spans (topic and source)
        spans = article.find_all('span')
        if len(spans) > 0:
            spans[0].string = news_item["topic"]
        if len(spans) > 1:
            spans[1].string = news_item["source"]

        if article.find('img'):
            article.find('img')['src'] = f"https://picsum.photos/seed/{news_item['title']}/800/400"
        html += str(article)

    html += html_end
    return html



def send_email(user, news):
    content_html = prepare_news_letter(news)
    url = 'https://api.novu.co/v1/events/trigger'
    headers = {
        'Authorization': 'ApiKey ' + novu_key,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    data = {
        "name": "emailerworkflow",
        "to": {
            "subscriberId": str(uuid.uuid4()),
            "email": user
        },
        "payload": ({'Message': content_html })
    }

    # Send the POST request
    http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=where())
    encoded_data = json.dumps(data).encode('utf-8')
    response = http.request('POST', url, headers=headers, body=encoded_data)

    return response






def find_user_and_news():
    user_dict=defaultdict() #key:email_id, value:topics
    for user in user_preferences_db.find():
        usert = json.loads(user['preference'])
        user_dict[user["user_id"]]=usert["topics"]

    latest_news = list(all_news_db.find({}, {"_id": 0}).sort([("_id", -1)]).limit(3))
    for user in user_dict.keys():
        user_realted_news=[]
        for news_item in latest_news:
            for i,j in news_item.items():
                for news in j:
                    if news["topic"] in user_dict[user]:
                        news_dict={
                            "title":news["title"],
                            "summary":news["summary"],
                            "link":news["link"],
                            "topic":news["topic"],
                            "source":i
                        }
                        user_realted_news.append(news_dict)
        if user_realted_news:
            send_email(user,user_realted_news)
            # print("Sending email to user",user)

if __name__ == "__main__":
    while True:
        find_user_and_news()
        #resend in 12 hours
        time.sleep(43200)




