from elasticsearch import Elasticsearch
import ssl

ELASTICSEARCH_HOST = f"https://localhost:9200"  

class ElasticClient(Elasticsearch):
    def __init__(self):
        hosts = [ELASTICSEARCH_HOST]
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        self.cnt = 0
        super().__init__(hosts=hosts, api_key=("xxxxxx_api_id", "xxxx_api_key"), ssl_context=context,  timeout=360)