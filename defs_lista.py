import httpx
import zipfile
import asyncio
from parsel import Selector
import os
import pandas as pd
import glob
from time import sleep

client = httpx.Client(proxy='http://172.64.68.249:80')
# def verificarCEP():
#     print("Verificando CEPs...")

def pegarproxy():
    proxy = client.get('http://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/countries/US/data.txt')
    print(proxy.headers)
    print(proxy.text)
    response = client.get('http://www.receitaws.com.br/v1/cnpj/07409896000106')
    if response.status_code == 200:
        dados = response.json()
        if dados["status"] == "OK":
            print("cnpj válido")
        else:
            print("cnpj inválido")
    else:
        print(f'erro {response.status_code} ao fazer requisição, {response.text}')
    
def verificarCNPJ():
    cnpjs = pd.read_csv('combined_file.csv', usecols=[1], dtype=str)
    for cnpj in cnpjs.iloc[:, 0]:
        response = client.get(f'https://www.receitaws.com.br/v1/cnpj/{cnpj}')
        if response.status_code == 200:
            dados = response.json()
            if dados["status"] == "OK":
                print("cnpj válido")
            else:
                print("cnpj inválido")
        else:
            print(f'erro {response.status_code} ao fazer requisição, {response.text}')

def selecao(df): # Seleciona as colunas de interesse e formata os dados
    novo_df = pd.DataFrame({
        0: df[0].astype(str) + df[1].astype(str) + df[2].astype(str),#CNPJ
        1: df[4],#NOME
        2: df[18].astype(str),#CEP
        3: df[14].astype(str) + "," + df[15].astype(str) + "," + df[16].astype(str) + "," + df[17].astype(str) + "," + df[20].astype(str),#endereço
        4: df[21].astype(str) + df[22].astype(str),#DDD 1 + TELEFONE 1
        5: df[23].astype(str) + df[24].astype(str),#DDD 2 + TELEFONE 2
        6: df[27]#CORREIO ELETRÔNICO
    })
    return novo_df

def filtros(df): # Filtra os dados para manter apenas os estabelecimentos ativos e com nome
    filtro_ativo = df[5] == '02'
    filtro_nome = (df[4] != "") & (df[4].notna())
    lista = df[filtro_ativo & filtro_nome]
    return lista

def juntar_csvs(): # Junta os arquivos CSV extraídos em um único arquivo
    arquivos = glob.glob("extracted_zip/*.ESTABELE")
    lista_dfs = []
    for file in arquivos:
        df = pd.read_csv(file, sep=';', header=None, encoding='latin1', dtype=str)
        dfs_comnome = selecao(filtros(df))
        lista_dfs.append(dfs_comnome)
    combined_df = pd.concat(lista_dfs, ignore_index=True)
    combined_df.to_csv('combined_file.csv', header=["CNPJ", "NOME", "CEP", "ENDEREÇO","TELEFONE 1","TELEFONE 2", "CORREIO ELETRÔNICO" ])
    print("CSV mesclado e salvo como 'combined_file.csv'")

def excel(): # Converte o CSV combinado em um arquivo Excel
    excel_df = pd.read_csv('combined_file.csv')
    excel = pd.ExcelWriter('combined_file.xlsx')
    excel_df.to_excel(excel, index=False)
    excel.close()

async def baixar(client, caminho): # Baixa e extrai os arquivos ZIP
    print(f'baixando {caminho}...')
    resp = await client.get(f'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-06/{caminho}')
    if resp.status_code != 200:
        print("Deu errado com status ",resp.status_code, "em ", caminho)
    print(f'baixou {caminho}/n')
    with open("arquivos_zip/" + caminho, mode="wb") as f:
        f.write(resp.content)
    print(f"salvou {caminho}")
    with zipfile.ZipFile("arquivos_zip/" + caminho, "r") as zfile:
        for filename in zfile.namelist():
            zfile.extract(filename, path="extracted_zip/")
            print(f"extraído: {filename}")