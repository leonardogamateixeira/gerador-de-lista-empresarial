import httpx
import zipfile
import asyncio
from parsel import Selector
import os
import pandas as pd
import glob
import requests
from time import sleep
from defs_lista import *

async def main():
    os.makedirs("arquivos_zip", exist_ok=True)
    os.makedirs("extracted_zip", exist_ok=True)
    client = httpx.AsyncClient()
    response = await client.get('https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-06/')
    sel = Selector(response.text)
    lista_de_hrefs = sel.xpath("//tr/td/a/@href").extract()
    coros = [baixar(client, link) for link in lista_de_hrefs if "Estabelecimento" in link]
    await asyncio.gather(*coros)
    juntar_csvs()
    verificarCNPJ()
    # verificarCEP()
    excel()
    
if __name__ == '__main__':
    # asyncio.run(main())
    # juntar_csvs()
    # verificarCNPJ()
    # pegarproxy()
