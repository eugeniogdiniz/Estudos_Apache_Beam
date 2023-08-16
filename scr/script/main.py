import re
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions

pipeline_options =  PipelineOptions(argc=None)
pipeline = beam.Pipeline(options=pipeline_options)

colunas_dengue = ['id',
                'data_iniSE',
                'casos',
                'ibge_code',
                'cidade',
                'uf',
                'cep',
                'latitude',
                'longitude']

def lista_para_dicionario(elemento, colunas):
    return dict(zip(colunas, elemento))


def texto_para_lista(elemento, delimitador='|'):
    """
    Recebe um texto e um delimitador e 
    retorna uma lista de elementos pelo delimitador
    """
    return elemento.split(delimitador)

def trata_data(elemento):
    """
    Recebe um dicionário e cria um novo campo com ANO-MES
    Retorna o mesmo dicionario com o novo campo
    """
    elemento['ano_mes'] = '-'.join(elemento['data_iniSE'].split('-')[:2])
    return elemento    

def chave_uf(elemento):
    """"
    Recebe um dicionario e Retornar uma tupla com o estado(UF)
    """
    chave = elemento['uf']
    return (chave, elemento)

def casos_dengue(elemento):
    """
    Recebe uma tupla
    """
    uf, registros = elemento
    for registro in registros:
        if bool(re.search(r'\d', registro['casos'])):
           yield (f"{uf}-{registro['ano_mes']}", float(registro['casos']))
        else:
            yield (f"{uf}-{registro['ano_mes']}", 0.0)

def chave_uf_ano_mes_de_lista(elemento):
    """
    Receber uma lista
    Retorna uma tupla trata
    """
    data, mm, uf = elemento
    ano_mes = '-'.join(data.split('-')[:2])
    chave = f'{uf}-{ano_mes}'
    if float(mm) <0:
        mm = 0.0
    else:
        mm = float(mm)
    return chave, mm

""" dengue = (
    pipeline
    | "Leitura do dataset de dengue" >> 
        ReadFromText('bases/casos_dengue.txt', skip_header_lines=1)
    | "De texto para lista" >> beam.Map(texto_para_lista)
    | "De lista para dicionario" >> beam.Map(lista_para_dicionario, colunas_dengue)
    | "Criar campo ano_mes" >> beam.Map(trata_data)
    | "Criar chave pelo estado" >> beam.Map(chave_uf)
    | "Agurpar pelo estado" >> beam.GroupByKey()
    | "Descompactar casos de dengue" >> beam.FlatMap(casos_dengue)
    | "Soma dos casos pela chave" >> beam.CombinePerKey(sum)
    | "Mostrar resultados" >> beam.Map(print)
) """

chuva = (
    pipeline
    | "Leitura do dataset de chuvas" >> 
        ReadFromText('bases/chuvas.csv', skip_header_lines=1)
    | "De texto para lista (chuvas)" >> beam.Map(texto_para_lista, delimitador=',')
    | "Criando a chave UF_ANO_MES" >> beam.Map(chave_uf_ano_mes_de_lista)
    | "Soma das chuvas pela chave" >> beam.CombinePerKey(sum)
    | "Mostrar resultados" >> beam.Map(print)
)


pipeline.run()



