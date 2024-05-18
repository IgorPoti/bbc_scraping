from google.cloud import bigquery
from google.api_core.exceptions import BadRequest
from fastapi import FastAPI, Query
from dotenv import load_dotenv
import os

load_dotenv()
app = FastAPI()


# Obtém o caminho para o arquivo JSON de credenciais
credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")


@app.get("/")
async def get_all_articles():
    client = bigquery.Client.from_service_account_json(json_credentials_path=credentials_path)
    job = client.query('select * from bbc_news_scrap.articles')
    
    results = job.result()
    return [dict(row) for row in results]


@app.get("/articles")
async def get_articles_by_tags(search: str = Query('brasil')):
    
    client = bigquery.Client.from_service_account_json(json_credentials_path=credentials_path)
    
    query = """
        SELECT * FROM bbc_news_scrap.articles
        WHERE LOWER(@search) IN (
            SELECT LOWER(tag) FROM UNNEST(list_tags) AS tag
        )
    """
    
    # Configura os parâmetros da consulta
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("search", "STRING", search.lower())
        ]
    )
    
    try:
        job = client.query(query, job_config=job_config)
        results = job.result()
        
        return [dict(row) for row in results]
    except BadRequest as e:
        
        return {"error": f"Erro ao executar a consulta: {e}"}