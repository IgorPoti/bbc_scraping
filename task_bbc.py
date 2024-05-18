import requests
from bs4 import BeautifulSoup as bs
import json


class GetDataFromBBC:
    
    
    def __init__(self):
        pass
    
    
    def _execute(self):
        # urls = self.get_urls()
        self.extract_article()
    
    
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


    def extract_article(self) -> dict:
        
        print('Inicializando extração de artigos!')
        urls = ["https://www.bbc.com/portuguese/articles/c4n1gx9pkw4o"]
        
        list_articles = []
        
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

            # Inicializar uma lista para armazenar os links de imagem encontrados
            image_links = []

            # Iterar sobre cada div encontrada
            for div in article_divs:
                # Encontrar a tag <picture> que precede a div que contém a imagem
                picture_tag = div.find_previous_sibling('figure', class_='bbc-1qn0xuy')
                
                if picture_tag:
                    # Encontrar a tag <img> dentro da tag <picture>
                    img_tag = picture_tag.find('img')

                    if img_tag:
                        img_link = img_tag['src']
                        if img_link not in image_links:
                            article_text.append(img_link)
                            image_links.append(img_link)
                
                paragraphs = div.find_all(['p', 'h2'])
                
                for element in paragraphs:
                    if element.name != 'div':
                        article_text.append(element.get_text(strip=True))
            
            article_dict = {
                'url': url,
                'headline': tags_json.get('@graph', [{}])[0].get('headline', ''),
                'description': tags_json.get('@graph', [{}])[0].get('description', ''),
                'date_published': tags_json.get('@graph', [{}])[0].get('datePublished', ''),
                'date_modified': tags_json.get('@graph', [{}])[0].get('dateModified', ''),
                'author': tags_json.get('@graph', [{}])[0]['author'].get('name', ''),
                'sameAs': tags_json.get('@graph', [{}])[0]['author'].get('sameAs', [''])[0],
                'in_language_name': tags_json.get('@graph', [{}])[0]['inLanguage'].get('name', ''),
                'in_language_alternate_name': tags_json.get('@graph', [{}])[0]['inLanguage'].get('alternateName', ''),
                'title_tag': title_tag,
                'list_tags': list_tags,
                'image_urls': image_urls,
                'article_text': article_text
            }

            list_articles.append(article_dict)            
                        
            print(article_dict)       

if __name__ == "__main__":
    scraper = GetDataFromBBC()
    scraper._execute()   

