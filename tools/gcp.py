from google.cloud import bigquery
from google.api_core.exceptions import NotFound

# Função para criar o dataset se não existir
def create_dataset_if_not_exists(client, dataset_id, project_id):
    try:
        client.get_dataset(dataset_id)  # Faz uma solicitação à API.
        print(f'Dataset {dataset_id} já existe.')
    except NotFound:
        dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")
        dataset.location = "southamerica-east1"
        dataset = client.create_dataset(dataset)  # Faz uma solicitação à API.
        print(f'Dataset {dataset_id} criado.')

# Função para criar a tabela se não existir
def create_table_if_not_exists(client, dataset_id, table_id):
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    try:
        client.get_table(table_ref)
        print(f'Table {table_id} já existe.')
    except NotFound:
        schema = [
            bigquery.SchemaField('url', 'STRING'),
            bigquery.SchemaField('headline', 'STRING'),
            bigquery.SchemaField('description', 'STRING'),
            bigquery.SchemaField('date_published', 'TIMESTAMP'),
            bigquery.SchemaField('date_modified', 'TIMESTAMP'),
            bigquery.SchemaField('author', 'STRING'),
            bigquery.SchemaField('sameAs', 'STRING'),
            bigquery.SchemaField('in_language_name', 'STRING'),
            bigquery.SchemaField('in_language_alternate_name', 'STRING'),
            bigquery.SchemaField('title_tag', 'STRING'),
            bigquery.SchemaField('list_tags', 'STRING', mode='REPEATED'),
            bigquery.SchemaField('image_urls', 'STRING', mode='REPEATED'),
            bigquery.SchemaField('article_text', 'STRING')
        ]
        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)
        print(f'Table {table_id} criado.')