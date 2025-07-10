# Data Warehouse - Acidentes de Trabalho

Este projeto implementa um Data Warehouse para análise de acidentes de trabalho usando PostgreSQL e Python.

## Estrutura do Projeto

```
projeto-final-tcs/
├── data/                     # Arquivos CSV com dados de acidentes
│   ├── D.SDA.PDA.005.CAT.202501.csv
│   ├── D.SDA.PDA.005.CAT.202502.csv
│   └── ...
├── db/                       # Scripts de banco de dados
│   └── access.py             # Script de setup 
│   └── core.py               # Script de setup
│   └── staging.py            # Script de setup
├── requirements.txt         # Dependências Python
├── .env.example            # Exemplo de configuração
└── README.md               # Este arquivo
```

## Arquitetura do Data Warehouse

### Schemas

1. **schema_staging**: Área de staging para dados brutos
2. **schema_core**: Área de dados processados e modelados
3. **schema_access**: Área de acesso para relatórios e dashboards

### Tabela Staging

A tabela `schema_staging.acidentes_trabalho` contém os seguintes campos padronizados:

- `id`: Chave primária sequencial
- `agente_causador_acidente`: Agente causador do acidente
- `data_acidente`: Data do acidente
- `cbo_codigo`: Código CBO
- `cbo_descricao`: Descrição CBO
- `cid_10_codigo`: Código CID-10
- `cid_10_descricao`: Descrição CID-10
- `cnae_empregador_codigo`: Código CNAE do empregador
- `cnae_empregador_descricao`: Descrição CNAE do empregador
- `emitente_cat`: Emitente da CAT
- `especie_beneficio`: Espécie do benefício
- `filiacao_segurado`: Filiação do segurado
- `indica_obito_acidente`: Indica óbito por acidente
- `municipio_empregador`: Município do empregador
- `natureza_lesao`: Natureza da lesão
- `origem_cadastramento_cat`: Origem do cadastramento da CAT
- `parte_corpo_atingida`: Parte do corpo atingida
- `sexo`: Sexo do acidentado
- `tipo_acidente`: Tipo do acidente
- `uf_municipio_acidente`: UF do município do acidente
- `uf_municipio_empregador`: UF do município do empregador
- `data_afastamento`: Data do afastamento
- `data_despacho_beneficio`: Data do despacho do benefício
- `data_acidente_duplicada`: Data do acidente (duplicada)
- `data_nascimento`: Data de nascimento
- `data_emissao_cat`: Data de emissão da CAT
- `tipo_empregador`: Tipo do empregador
- `cnpj_cei_empregador`: CNPJ/CEI do empregador
- `arquivo_origem`: Nome do arquivo CSV de origem
- `data_carga`: Data e hora da carga

## Pré-requisitos

1. **PostgreSQL**: Instalar PostgreSQL (versão 12 ou superior)
2. **Python**: Python 3.8 ou superior
3. **Dependências Python**: Instalar via requirements.txt

## Instalação

### 1. Clonar o repositório
```bash
git clone <url-do-repositorio>
cd projeto-final-tcs
```

### 2. Instalar dependências Python
```bash
pip install -r requirements.txt
```

### 3. Configurar banco de dados
```bash
# Copiar arquivo de configuração
cp .env.example .env

# Editar .env com suas configurações
notepad .env
```

### 4. Executar o setup
```bash
python db/banco.py
```

## Uso

### Executar o Script Principal

```bash
python db/banco.py
```

O script irá:
1. Criar o banco de dados `acidentes_trabalho_dw`
2. Criar os schemas `schema_staging`, `schema_core`, `schema_access`
3. Criar a tabela `schema_staging.acidentes_trabalho`
4. Carregar todos os arquivos CSV da pasta `data/`
5. Exibir estatísticas dos dados carregados

### Personalizar Configurações

Edite o arquivo `.env` ou modifique diretamente as configurações na classe `DataWarehouseSetup`:

```python
dw_setup = DataWarehouseSetup(
    host='localhost',
    port=5432,
    database='acidentes_trabalho_dw',
    user='postgres',
    password='sua_senha'
)
```

## Funcionalidades

### Limpeza e Normalização de Dados

- Padronização de nomes de colunas (snake_case)
- Conversão de datas para formato adequado
- Tratamento de valores nulos e inválidos
- Normalização de campos categóricos

### Controle de Qualidade

- Constraints para campos críticos
- Índices para otimização de consultas
- Logging detalhado do processo
- Estatísticas de carga

### Monitoramento

- Logs detalhados do processo
- Estatísticas por arquivo processado
- Controle de registros duplicados
- Rastreamento de origem dos dados

## Estrutura de Dados

Os dados são provenientes de arquivos CSV com informações de acidentes de trabalho, incluindo:

- Dados do acidente (data, tipo, agente causador)
- Informações do trabalhador (sexo, CBO, data nascimento)
- Dados do empregador (CNAE, município, UF)
- Informações médicas (CID-10, natureza da lesão)
- Dados administrativos (CAT, benefícios)

## Próximos Passos

1. **Schema Core**: Implementar modelagem dimensional
2. **ETL**: Criar processos de transformação e limpeza
3. **Schema Access**: Criar views e tabelas para relatórios
4. **Dashboard**: Implementar visualizações
5. **Automação**: Criar jobs para carga incremental

## Contribuição

1. Fork o projeto
2. Crie uma branch para sua feature
3. Commit suas mudanças
4. Push para a branch
5. Abra um Pull Request

## Licença

Este projeto está sob a licença MIT.
