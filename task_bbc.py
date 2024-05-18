import requests
from bs4 import BeautifulSoup as bs
import json


class GetDataFromBBC:
    def __init__(self):
        pass
    
    
    def _execute(self):
        urls = self.get_urls()
        self.extract_article(urls)
    
    
    def get_urls(self) -> list:
        print('Buscando artigos!')
        response = requests.get("https://www.bbc.com/portuguese")
        soup = bs(response.content, 'html.parser')

        urls = []

        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            
            if href.startswith('https://www.bbc.com/portuguese/articles/'):
                urls.append(href)
                print(href)

        return urls


    def extract_article(self, urls: list) -> dict:
        
        print('Inicializando extração de artigos!')
        for url in urls:
            print(f'Extraindo artigo: {url}')
            response = requests.get(url)

            soup = bs(response.content, 'html.parser')

            meta_tag_titulo_pagina = soup.find('meta', {'data-react-helmet': 'true', 'property': 'og:image'}).get('content')
            meta_tags_article_tag = soup.find_all('meta', {'data-react-helmet': 'true', 'name': 'article:tag'})


            script_tag = soup.find('script', {'data-react-helmet': 'true', 'type': 'application/ld+json'})

            img_tags = soup.select('figure.bbc-1qn0xuy img')

            tags_json = json.loads(script_tag.text)

            # Tags principais da noticia

            url = url
            headline = tags_json.get('@graph', [{}])[0].get('headline', '')
            description = tags_json.get('@graph', [{}])[0].get('description', '')
            date_published = tags_json.get('@graph', [{}])[0].get('datePublished', '')
            date_modified = tags_json.get('@graph', [{}])[0].get('dateModified', '')
            author = tags_json.get('@graph', [{}])[0]['author'].get('name', '')
            sameAs = tags_json.get('@graph', [{}])[0]['author'].get('sameAs', [''])[0]
            in_language_name = tags_json.get('@graph', [{}])[0]['inLanguage'].get('name', '')
            in_language_alternate_name = tags_json.get('@graph', [{}])[0]['inLanguage'].get('alternateName', '')

            title_tag = soup.find('title').text
            list_tags = [tag.get('content', '') for tag in meta_tags_article_tag]
            image_urls = [img['src'] for img in img_tags]

            article_divs = soup.find_all('div', class_='bbc-19j92fr ebmt73l0')


            article_text = []

            for div in article_divs:
                paragraphs = div.find_all(['p', 'h2'])  
                
                for element in paragraphs:
                    article_text.append(element.get_text(strip=True))
            

if __name__ == "__main__":
    scraper = GetDataFromBBC()
    scraper._execute()   

