from bs4 import BeautifulSoup
import requests
import re
import json
from urllib.parse import urljoin
import mysql.connector


def scrape_meduza():
    mydb = mysql.connector.connect(
        host="localhost",
        user="alexx",
        password="1606",
        database="article_scraper"
    )

    mycursor = mydb.cursor()

    heading = []
    article = []

    url = 'https://meduza.io/en'
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'}

    page = requests.get(url, headers=headers)
    soup = BeautifulSoup(page.content, 'html.parser')

    link_tags = soup.find_all('a', class_='Link-module-root Link-module-isInBlockTitle')

    release_links = []
    for link_tag in link_tags:
        release_links.append(link_tag.get('href'))

    article_urls = []
    for link in release_links:
        article_urls.append(urljoin(url, link))

    for i in article_urls:
        response = requests.get(i, headers=headers)
        soup = BeautifulSoup(response.content, 'html.parser')

        try:
            heading_text = soup.find('h1', class_='RichTitle-module_root__U5XQu').text
            heading_cleaned = re.sub(r'\n\s+|\\u[0-9a-fA-F]{4}', '', heading_text)
            heading.append(heading_cleaned)
        except:
            heading.append('')

        paragraphs = []
        try:
            article_body_div = soup.find('div', class_='GeneralMaterial-module-container')
            for paragraph in article_body_div.find_all('p'):
                if paragraph.text.strip():
                    paragraph_text = re.sub(
                        r'\n\s+|\\u[0-9a-fA-F]{4}|&nbsp;|\u00a0', '', paragraph.text)
                    paragraphs.append(paragraph_text)
        except:
            pass

        article.append(paragraphs)

    meduza_articles = []
    for i in range(len(article_urls)):
        meduza_articles.append({
            "source": url,
            "url": article_urls[i],
            "title": heading[i],
            "content": article[i]
        })

    for data in meduza_articles:
        sql = "INSERT INTO articles (source, url, title, content) VALUES (%s, %s, %s, %s)"
        values = (data["source"], data["url"], data["title"], str(data["content"]))
        mycursor.execute(sql, values)

    mydb.commit()

    mydb.close()
