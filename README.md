# Dívida PGFN
A Procuradoria Geral da Fazenda Nacional (PGFN), seguindo os preceitos da Lei de Acesso à Informação (LAI), disponibiliza em seu sítio eletrônico a [base de dados completa dos devedores inscritos em dívida ativa da união e do FGTS](https://www.gov.br/pgfn/pt-br/assuntos/divida-ativa-da-uniao/dados-abertos/dados-abertos). Esses arquivos, em formato CSV são separados pelo sistema originário (FGTS, Previdenciário e Não Previdenciário) e por Unidade da Federação (UF).

Para facilitar a análise dos dados, este programa junta todos estes arquivos disponibilizados separadamente em apenas uma base de dados SQLite.

Em uma segunda etapa, o programa é capaz de selecionar detalhes dessa base de dados e exportar em arquivos CSV menores para posterior análise.

# Uso

## Estrutura de diretórios

```
Diretório base
├── entrada
│   ├── FGTS
│   ├── Nao_Previdenciario
│   └── Previdenciario
└── saida
```

Deve ser criado um diretório base para as entradas e saídas do programa. Os arquivos baixados no [sítio da PGFN](https://www.gov.br/pgfn/pt-br/assuntos/divida-ativa-da-uniao/dados-abertos/dados-abertos) devem ser extraídos nos respectivos diretórios de entrada (FGTS, Previdenciário e Não Previdenciário).

## Criação da base consolidada

`DividaPGFN base -d=<diretório base>`

Gera uma base consolidada SQLite a partir dos arquivos disponilizados pela PGFN.

## Exportação dos arquivos de análise

`DividaPGFN filtro -d=<diretório base> -b=<base CNPJ> -c=<consulta SQL de filtro>`

Com o uso da base auxiliar do CNPJ disponibilizada pelo projeto [cnpj-sqlite](https://github.com/rictom/cnpj-sqlite), exporta arquivos extraídos da base da dívida, para análises posteriores. Os registros que serão filtrados no banco de dados dependem de consulta SQL definida pelo usuário, que é realizada na base auxiliar do CNPJ.

* saida\divida.csv: Registros desagregados da dívida como dispobilizados pela PGFN;
* saida\corresponsaveis.csv: Dados complementares de corresponsáveis pelas dívidas que foram extraídas da base;
* saida\cnpj.csv: Extração da base do CNPJ com os estabelecimentos extraídos;
* saida\socios.csv: Sócios das empresas constantes no arquivo de CNPJs.

### Metodologia

A consulta SQL definida pelo usuário é responsável por listar os CNPJs relevantes que se encaixam nos critérios esperados.

Com essa lista, são extraídas da base da dívida, as inscrições da dívida em que esses CNPJs apareçam como responsável ou corresponsável.

Com esse conjunto de todas as inscrições da dívida relevantes ao usuário, é listado o conjunto expandido de CNPJs envolvidos como devedor principal ou secundário. Isso permite capturar situações em que algum devedor, principal ou secundário, não se encaixa no filtro inicial de CNPJs definidos pelo usuário, mas figura como co-devedor de algum CNPJ presente no filtro inicial.

Por fim, todas as extrações da base das dívidas e da base do CNPJ são realizadas com esse conjunto expandido de CNPJs.

Para ilustrar, um exemplo, com as dívidas dos clubes sociais, esportivos e similares é disponibilizado no diretório de exemplos. 
