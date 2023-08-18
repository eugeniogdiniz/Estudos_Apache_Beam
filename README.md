# Estudos_Apache_Beam

## Praticando

A prática é muito importante para fixação dos conteúdos estudados. Chegou a hora de você seguir todos os passos realizados por mim durante esta aula:

* Descompactar o que é chave, de dados vindos em um dicionário;
Pegar os valores nos dicionários;
Aplicar o método split para criar uma lista e atribuir cada valor à sua respectiva variável;
* Retornar os elementos em uma tupla, para que o dado seja uma única string que possa ser escrita em um arquivo;
Receber e aplicar a tupla em um join, que vai concatenar os elementos e inserir uma string;
* Chamar os dois métodos a partir de Maps e consolidar o dado para ser persistido, em um arquivo csv.

Caso já tenha feito, excelente. Se ainda não, é importante que você implemente o que foi visto em vídeo para poder continuar com a próxima aula, que tem como pré-requisito todo o código escrito nesta.

Se, por acaso, você já domina essa parte, no início de cada aula (a partir da segunda), você poderá baixar o projeto feito até aquele ponto.

Lembre-se que você pode personalizar seu projeto como quiser e testar alternativas à vontade. O importante é acompanhar a construção do projeto na prática, a partir do que é estudado em aula.

Desde o início do projeto, na fase de análise, definimos uma chave para agregar as informações contidas nos dois arquivos brutos, com objetivo de tirar mais proveito das informações, possibilitando correlacionar os arquivos.

Ao final do processo, precisamos desconstruir essa chave e criar elementos separados a partir dela. Então, a string UF-ANO-MES passaria a se tornar três elementos distintos:

* UF;
* ANO;
* MES;

Para isso, criamos um método para realizar essa descompactação, bem como retirar os valores que não correspondiam à essa chave dos campos de chuva e dengue. Então, resolvemos tudo isso em um método só.

Primeiro, descompactando o que é chave, de dados vindos em um dicionário. Na sequência, pegando os valores nos dicionários. E por fim, aplicando novamente o método split para criar uma lista e atribuir cada valor à sua respectiva variável. O código ficou assim:

    def descompactar_elementos(elem):
        """
        Receber uma tupla ('CE-2015-11', {'chuvas': [0.4], 'dengue': [21.0]})
        Retornar uma tupla ('CE', '2015', '11', '0.4', '21.0')
        """
        chave, dados = elem
        chuva = dados['chuvas'][0]
        dengue = dados['dengue'][0]
        uf, ano, mes = chave.split('-')
        return uf, ano, mes, str(chuva), str(dengue)

Ao final retornamos os elementos em uma tupla, para que no passo seguinte esse dado seja composto em uma única string ser escrita em um arquivo.

No método seguinte essa tupla foi recebida e aplicada a ela um join, que vai concatenar esses elementos e inserir uma string entre eles. No caso, o delimitador que queremos no arquivo csv é o ponto e vírgula (;). O código ficou da seguinte forma:

    def preparar_csv(elem, delimitador=';'):
        """
        Receber uma tupla ('CE', 2015, 11, 0.4, 21.0)
        Retornar uma string delimitada "CE;2015;11;0.4;21.0"
        """
        return f"{delimitador}".join(elem)

Agora conseguimos chamar os dois métodos a partir de Maps e consolidar o dado para ser persistido, em um arquivo csv.

    resultado = (
        # (chuvas, dengue)
        # | "Empilha as pcols" >> beam.Flatten()
        # | "Agrupa as pcols" >> beam.GroupByKey()
        ({'chuvas': chuvas, 'dengue': dengue})
        | 'Mesclar pcols' >> beam.CoGroupByKey()
        | 'Filtrar dados vazios' >> beam.Filter(filtra_campos_vazios)
        | 'Descompactar elementos' >> beam.Map(descompactar_elementos)
        | 'Preparar csv' >> beam.Map(preparar_csv)
        # | "Mostrar resultados da união" >> beam.Map(print)
    )

## Sobre as conversões

E eu vou criar fora do resultado uma nova pipeline utilizando essa pcollection resultado. Então resultado, vou criar uma pipeline, um processo a partir dele. Eu vou colocar aqui o nome, que é: ’Criar arquivo CSV’, que é o que nós vamos fazer, maior que, maior que e aqui nós temos que chamar quem vai ser responsável por converter essa nossa pcollection em um arquivo de texto, no caso, um arquivo CSV. Esse é o WriteToText.

Temos que fazer a importação, ele fica junto do mesmo local lá do io, onde está o ReadFromText. Ele já importou aqui para mim, nós temos aqui from apache_bem.io.textio import WriteToText. E se nós colocarmos vírgula aqui, deixa eu mostrar lá embaixo, vou apertar “Ctrl + espaço”, várias coisas que nós podemos escrever.

![Alt text](<Captura de tela de 2023-08-17 21-31-20.png>)

Eu tenho como escrever um PubSub, que é uma mensageria, tem o Parquet, que já vai ser um arquivo estruturado, em um banco, no caso o Mongo, BigQuery, que já vai ser da infraestrutura do Google, um Avro, que é um arquivo mais compactado que o Parquet, mas também é estruturado.

