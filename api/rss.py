import feedparser
import requests

rss_url = "https://feeds.bbci.co.uk/news/rss.xml"
headers = {
    "User-Agent": "Mozilla/5.0"
}

response = requests.get(rss_url, headers=headers)

feed = feedparser.parse(response.content)

news = []

for entry in feed.entries:
    news.append({
        "title": entry.title,
        "link": entry.link,
        "published": entry.get("published", ""),
        "summary": entry.get("summary", "")
    })

print(len(news))
print(news[:3])