import re
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.textio import WriteToText
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

def arredonda(elemento):
    """"
    Arredonda valores de uma tupla
    """
    chave, mm = elemento
    return (chave, round(mm, 1))

def filtra_dados_vazios(elemento):
    """
    Remove elementos que tenham chaves vazias
    """
    chave, dados = elemento
    if all([
        dados['chuvas'],
        dados['dengue']
        ]):
        return True
    return False

def descompactar_elementos(elemento):
    """
    Vai receber uma tupla e retornar uma lista
    """
    chave, dados = elemento
    chuva = dados['chuvas'][0]
    dengue = dados['dengue'][0]
    uf, ano, mes = chave.split('-')
    return uf, ano, mes, str(chuva), str(dengue)

def preparando_csv(elemento, delimitador=';'):
    """
    Recebe uma tupla e retorna uma string delimitada
    """
    return f"{delimitador}".join(elemento)

dengue = (
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
#    | "Mostrar resultados" >> beam.Map(print)
) 

chuva = (
    pipeline
    | "Leitura do dataset de chuvas" >> 
        ReadFromText('bases/chuvas.csv', skip_header_lines=1)
    | "De texto para lista (chuvas)" >> beam.Map(texto_para_lista, delimitador=',')
    | "Criando a chave UF_ANO_MES" >> beam.Map(chave_uf_ano_mes_de_lista)
    | "Soma do total de chuvas pela chave" >> beam.CombinePerKey(sum)
    | "Arredondar resultados de chuvas" >> beam.Map(arredonda)
#    | "Mostrar resultados" >> beam.Map(print)
)

resultado = (
    # (chuva, dengue)
    # | "O metódo Flatten une as Pcollections" >> beam.Flatten()
    # | "Agrupando as Pcols" >> beam.GroupByKey()
    ({'chuvas': chuva, 'dengue': dengue})
    | "Mesclar pcols" >> beam.CoGroupByKey()
    | "filtrar dados vazios" >> beam.Filter(filtra_dados_vazios)
    | "Descompactar elementos" >> beam.Map(descompactar_elementos)
    | "Preparando CSV" >> beam.Map(preparando_csv)
    # | "Mostra o resultado da união" >> beam.Map(print)
)

#uf, ano, mes, str(chuva), str(dengue)

header = 'uf;ano;mes;chuva;dengue'

resultado | "Criar arquivo CSV" >> WriteToText('Resultado', file_name_suffix='.csv',\
                                               header=header)


pipeline.run()



