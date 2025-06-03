from .celery_app import celery
from .utils import process_cnpj_batch  # You will define this function

@celery.task
def scrape_cnpj_batch(cnpj_batch):
    return process_cnpj_batch(cnpj_batch)