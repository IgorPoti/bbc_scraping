import requests
import os
import json
from dotenv import load_dotenv
from bs4 import BeautifulSoup as bs

from google.cloud import bigquery

from tools.gcp import create_table_if_not_exists, create_dataset_if_not_exists


class GetDataFromBBC:
    
    
    def __init__(self):
        load_dotenv()
        credentials_path = os.getenv("GOOGLE_ACESS")
        # Autenticação e configuração do cliente BigQuery
        self.client = bigquery.Client.from_service_account_json(credentials_path)
    
    
    def _execute(self) -> None:
        """
        Executa o fluxo principal do programa.

        Etapas:
        1. Obtém os URLs dos artigos chamando o método get_urls.
        2. Extrai as informações dos artigos chamando o método extract_article com a lista de URLs como argumento.
        3. Envia os dados extraídos para o Google Cloud Platform utilizando o método send_data_to_gcp.

        Retorna:
        None.
        
        """
        
        urls = self.get_urls()
        list_articles = self.extract_article(urls)
        self.send_data_to_gcp(list_articles)
        
    
    def get_urls(self) -> list:

        """
        Obtém os URLs dos artigos disponíveis na página inicial do site da BBC News Brasil.

        Retorna:
        list: Uma lista de URLs dos artigos encontrados na página.

        A função faz uma solicitação HTTP para a página inicial do site da BBC em português, analisa o HTML utilizando BeautifulSoup e encontra todos os elementos de âncora ('a') que possuem um atributo 'href'. Em seguida, verifica se o valor do atributo 'href' começa com 'https://www.bbc.com/portuguese/articles/'. Se começar, adiciona o URL à lista de URLs. No final, retorna a lista contendo os URLs dos artigos encontrados.
        """
        
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


    def extract_article(self, urls) -> list:
        
        """
        Extrai informações de artigos a partir de uma lista de URLs.

        Argumentos:
        urls (list): Uma lista de URLs dos quais os artigos serão extraídos.

        Retorna:
        list

        A função percorre cada URL na lista fornecida, faz uma solicitação HTTP para a página, analisa o HTML usando BeautifulSoup, extrai informações relevantes do artigo e as organiza em um dicionário.
        """
    
        print('Inicializando extração de artigos!')
        
        list_articles = []
        
        for url in urls:
            print(f'Extraindo artigo: {url}')
            response = requests.get(url)

            soup = bs(response.content, 'html.parser')

            # meta_tag_titulo_pagina = soup.find('meta', {'data-react-helmet': 'true', 'property': 'og:image'}).get('content')
            meta_tags_article_tag = soup.find_all('meta', {'data-react-helmet': 'true', 'name': 'article:tag'})


            script_tag = soup.find('script', {'data-react-helmet': 'true', 'type': 'application/ld+json'})

            img_tags = soup.select('figure.bbc-1qn0xuy img')

            tags_json = json.loads(script_tag.text)

            title_tag = soup.find('title').text
            title_page = soup.find('div', class_='bbc-1151pbn ebmt73l0').text
            list_tags = [tag.get('content', '') for tag in meta_tags_article_tag]
            image_urls = [img['src'] for img in img_tags]

            article_divs = soup.find_all('div', class_='bbc-19j92fr ebmt73l0')

            article_text = []

            # Inicializar uma lista para armazenar os links de imagem encontrados
            image_links = []

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
            
            # Combinar o texto do artigo em uma única string com \n entre cada parágrafo
            article_text_combined = '\n'.join(article_text)
            
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
                'title_page': title_page,
                'list_tags': list_tags,
                'image_urls': image_urls,
                'article_text': article_text_combined
            }

            list_articles.append(article_dict)            
                        
        return list_articles
    
    
    def send_data_to_gcp(self, list_articles) -> None:
        """
        Envia os dados extraídos para o GCP através do Google BigQuery.

        Argumentos:
        list_articles (list): Uma lista de dicionários contendo dados sobre os artigos a serem enviados para o BigQuery.

        Retorna:
        None

        Esta função verifica se o dataset e a tabela especificados existem no BigQuery. Se não existirem, ela os cria. Em seguida, itera sobre cada artigo na lista fornecida e prepara os dados para inserção no BigQuery. Antes da inserção, a função verifica se já existe algum artigo com a mesma URL na tabela para evitar duplicatas. Se a URL do artigo ainda não existir na tabela, os dados do artigo são inseridos. Caso contrário, uma mensagem indicando que a URL já existe na tabela é exibida.
        """
        
        project_id = '899333236152'
        dataset_id = 'bbc_news_scrap'  
        table_id = 'articles'
        table_ref = f"{project_id}.{dataset_id}.{table_id}"


        create_dataset_if_not_exists(self.client, dataset_id, project_id)

        create_table_if_not_exists(self.client, dataset_id, table_id)

        # Verificação e envio dos dados para o BigQuery
        for article in list_articles:
            row_to_insert = {
                'url': article.get('url', ''),
                'headline': article.get('headline', ''),
                'description': article.get('description', ''),
                'date_published': article.get('date_published', None),
                'date_modified': article.get('date_modified', None),
                'author': article.get('author', ''),
                'sameAs': article.get('sameAs', ''),
                'in_language_name': article.get('in_language_name', ''),
                'in_language_alternate_name': article.get('in_language_alternate_name', ''),
                'title_tag': article.get('title_tag', ''),
                'title_page': article.get('title_page', ''),
                'list_tags': article.get('list_tags', []),
                'image_urls': article.get('image_urls', []),
                'article_text': article.get('article_text', '')
            }
            
            try:
                # Validar se já existe algum artigo inserido no BigQuery com a URL atual    
                query = f"SELECT COUNT(*) FROM {dataset_id}.{table_id} WHERE url = @url"

                job = self.client.query(
                    query,
                    job_config=bigquery.QueryJobConfig(
                        query_parameters=[bigquery.ScalarQueryParameter("url", "STRING", article['url'])]
                    )
                )
                
                result = job.result()

                row_count = list(result)[0][0]

                # Se a URL não existir, insere na tabela os dados do artigo
                if row_count == 0:
                    errors = self.client.insert_rows_json(table_ref, [row_to_insert], row_ids=[None])
                    if errors:
                        print(f"Erro ao inserir dados do artigo com URL {article['url']}:", errors)
                    else:
                        print(f"Dados do artigo com URL {article['url']} inseridos com sucesso.")
                else:
                    print(f"A URL {article['url']} já existe na tabela!")
                    
            except Exception as e:
                    print(f"Erro ao inserir dados do artigo com URL {article['url']}:", e)


if __name__ == "__main__":
    scraper = GetDataFromBBC()
    scraper._execute()
