import os

from google.cloud import bigquery
from google.api_core.exceptions import BadRequest
from fastapi import FastAPI, Query
from typing import List, Dict, Any
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title='BBC News Brasil ETL',
              description='Endpoints para consulta de artigos que passaram pelo processo de ETL de artigos da BBC News Brasil'
              )

credentials_path = os.getenv("GOOGLE_ACESS")

@app.get("/",
         description="Retorna todas as informações dos artigos",
         response_model=List[Dict[str, Any]]
         )
async def get_all_articles():
    client = bigquery.Client.from_service_account_json(json_credentials_path=credentials_path)
    job = client.query('select * from bbc_news_scrap.articles LIMIT 100')

    results = job.result()
    return [dict(row) for row in results]


@app.get("/articles",
         description='Retorna a lista de artigos com a tag inserida',
         response_model=List[Dict[str, Any]])
async def get_articles_by_tags(search: str = Query('brasil')):
    client = bigquery.Client.from_service_account_json(json_credentials_path=credentials_path)

    query = """
        SELECT * FROM bbc_news_scrap.articles
        WHERE LOWER(@search) IN (
            SELECT LOWER(tag) FROM UNNEST(list_tags) AS tag
        )
        LIMIT 10;
    """

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
