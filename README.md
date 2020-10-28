# Inspark
Uma solução que facilita a manutenção de códigos legados em Pyspark.

## O problema

O problema de lidar com código legado existe desde sempre, mas lidar com código feito usando pyspark tem se tornado comum principalmente quando o mesmo é feito usando práticas ruins de desenvolvimento. Existem normalmente duas maneiras de escrever código em pyspark. A primeira e mais comum é usando SQL puro e controlando o fluxo lógico com Python. A outra maneira mais difundida e considerada como boa prática é usando a API de DataFrame em que as operações de consulta e tratamento de dados são feitos usando classes e funções o que permite um controle do fluxo lógico mais robusto e reaproveitável.

Uma das diferenças entre as duas maneiras está na linearidade de leitura do código. O código escrito usado SQL puro costuma ser mais linear já que é mais dificil realizar modularizações ou procedimentos especificos. Já usando a API de DataFrame é utilizado estruturas funcionais ou orientadas a objeto para reaproveitamento o que torna o código modularizado o que é bom para o reaproveitamento de código, mas ruim quando o assunto é manutenção ou leitura de código.

O problema no final está na manutenção ou otimização de código legado feito em Pyspark usando a API de DataFrame poder ser muito complicada.


## A solução

A solução proposta é uma ferramenta capaz de analizar o código legado e estrutura-lo de maneira linear e concentrada apenas nas operações de dados eliminando qualquer controle do fluxo lógico. Permitindo que se identifique redundancias de operãções ou operações desnecessarias.

## A implementação

Para implementar essa solução será aplicado o conceito de Mock nas funções e classes da API de DataFrame do pyspark, assim quando executar o script legado toda a sequencia de operações da API poderá ser rastreada e posteriormente compilada num formato linear.

## Como usar

```python
from inspark import Refactoring
refact = Refactoring()
refact.mock()

# Comandos em pyspark

print(refact.output_text)
```

## Próximos passos

O próximo passo na melhoria da ferramenta é implementar uma otimização da sequência de comandos retornando menos variáveis de dataframe.
