import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import os
import glob
from sqlalchemy import create_engine
import logging
from datetime import datetime

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataWarehouseSetup:
    """Classe para configurar e gerenciar o Data Warehouse de acidentes de trabalho"""
    
    def __init__(self, 
                 host='localhost', 
                 port=5432, 
                 database='acidente_trabalho_dw', 
                 user='postgres', 
                 password='postgres'):
        """
        Inicializa a conexão com o banco de dados PostgreSQL
        
        Args:
            host (str): Host do PostgreSQL
            port (int): Porta do PostgreSQL
            database (str): Nome do banco de dados
            user (str): Usuário do PostgreSQL
            password (str): Senha do PostgreSQL
        """
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        
        # String de conexão para psycopg2
        self.conn_string = f"postgresql://{user}:{password}@{host}:{port}/{database}?client_encoding=latin1"
        
        # Engine do SQLAlchemy para operações com pandas
        self.engine = create_engine(self.conn_string, echo=False)
        
    def create_database_if_not_exists(self):
        """Cria o banco de dados se não existir"""
        try:
            # Conecta ao banco postgres padrão para criar o banco
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database='postgres',
                user=self.user,
                password=self.password,
                client_encoding='latin1'
            )
            conn.autocommit = True
            cursor = conn.cursor()
            
            # Verifica se o banco existe
            cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{self.database}'")
            exists = cursor.fetchone()
            
            if not exists:
                logger.info(f"Criando banco de dados: {self.database}")
                cursor.execute(f'CREATE DATABASE "{self.database}"')
                logger.info(f"Banco de dados {self.database} criado com sucesso!")
            else:
                logger.info(f"Banco de dados {self.database} já existe")
                
            cursor.close()
            conn.close()
            
        except Exception as e:
            logger.error(f"Erro ao criar banco de dados: {e}")
            raise
    
    def create_schemas(self):
        """Cria os schemas do Data Warehouse"""
        try:
            with psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                client_encoding='latin1'
            ) as conn:
                cursor = conn.cursor()
                
                schemas = ['schema_staging', 'schema_core', 'schema_access']
                
                for schema in schemas:
                    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
                    logger.info(f"Schema {schema} criado/verificado com sucesso")
                
                conn.commit()
                logger.info("Todos os schemas foram criados com sucesso!")
                
        except Exception as e:
            logger.error(f"Erro ao criar schemas: {e}")
            raise
    
    def create_staging_table(self):
        """Cria a tabela de staging para acidentes de trabalho"""
        try:
            with psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                client_encoding='latin1'
            ) as conn:
                cursor = conn.cursor()
                  # SQL para criar a tabela de staging (sem restrições para aceitar dados brutos)
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS schema_staging.acidente_trabalho (
                    id SERIAL PRIMARY KEY,
                    agente_causador_acidente TEXT,
                    data_acidente TEXT,
                    cbo_codigo TEXT,
                    cbo_descricao TEXT,
                    cid_10_codigo TEXT,
                    cid_10_descricao TEXT,
                    cnae_empregador_codigo TEXT,
                    cnae_empregador_descricao TEXT,
                    emitente_cat TEXT,
                    especie_beneficio TEXT,
                    filiacao_segurado TEXT,
                    indica_obito_acidente TEXT,
                    municipio_empregador TEXT,
                    natureza_lesao TEXT,
                    origem_cadastramento_cat TEXT,
                    parte_corpo_atingida TEXT,
                    sexo TEXT,
                    tipo_acidente TEXT,
                    uf_municipio_acidente TEXT,
                    uf_municipio_empregador TEXT,
                    data_afastamento TEXT,
                    data_despacho_beneficio TEXT,
                    data_acidente_duplicada TEXT,
                    data_nascimento TEXT,
                    data_emissao_cat TEXT,
                    tipo_empregador TEXT,
                    cnpj_cei_empregador TEXT,
                    arquivo_origem TEXT,
                    data_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
                
                cursor.execute(create_table_sql)
                
                # Criar índices para melhorar performance
                indices = [
                    "CREATE INDEX IF NOT EXISTS idx_acidentes_data_acidente ON schema_staging.acidente_trabalho(data_acidente);",
                    "CREATE INDEX IF NOT EXISTS idx_acidentes_municipio_empregador ON schema_staging.acidente_trabalho(municipio_empregador);",
                    "CREATE INDEX IF NOT EXISTS idx_acidentes_cnae ON schema_staging.acidente_trabalho(cnae_empregador_codigo);",
                    "CREATE INDEX IF NOT EXISTS idx_acidentes_sexo ON schema_staging.acidente_trabalho(sexo);",
                    "CREATE INDEX IF NOT EXISTS idx_acidentes_tipo ON schema_staging.acidente_trabalho(tipo_acidente);",
                    "CREATE INDEX IF NOT EXISTS idx_acidentes_arquivo_origem ON schema_staging.acidente_trabalho(arquivo_origem);"                ]
                
                for index_sql in indices:
                    cursor.execute(index_sql)
                
                conn.commit()
                logger.info("Tabela schema_staging.acidente_trabalho criada com sucesso!")
                
        except Exception as e:
            logger.error(f"Erro ao criar tabela de staging: {e}")
            raise
    
    def clean_and_normalize_data(self, df, arquivo_origem):
        """Apenas renomeia as colunas e adiciona origem do arquivo - sem normalização"""
        try:
            # Mapear colunas para nomes padronizados
            column_mapping = {
                'Agente  Causador  Acidente': 'agente_causador_acidente',
                'Data Acidente': 'data_acidente',
                'CBO': 'cbo_codigo',
                'CBO.1': 'cbo_descricao',
                'CID-10': 'cid_10_codigo',
                'CID-10.1': 'cid_10_descricao',
                'CNAE2.0 Empregador': 'cnae_empregador_codigo',
                'CNAE2.0 Empregador.1': 'cnae_empregador_descricao',
                'Emitente CAT': 'emitente_cat',
                'Espécie do benefício': 'especie_beneficio',
                'Filiação Segurado': 'filiacao_segurado',
                'Indica Óbito Acidente': 'indica_obito_acidente',
                'Munic Empr': 'municipio_empregador',
                'Natureza da Lesão': 'natureza_lesao',
                'Origem de Cadastramento CAT': 'origem_cadastramento_cat',
                'Parte Corpo Atingida': 'parte_corpo_atingida',
                'Sexo': 'sexo',
                'Tipo do Acidente': 'tipo_acidente',
                'UF  Munic.  Acidente': 'uf_municipio_acidente',
                'UF Munic. Empregador': 'uf_municipio_empregador',
                'Data  Afastamento': 'data_afastamento',
                'Data Despacho Benefício': 'data_despacho_beneficio',
                'Data Acidente.1': 'data_acidente_duplicada',
                'Data Nascimento': 'data_nascimento',
                'Data Emissão CAT': 'data_emissao_cat',
                'Tipo de Empregador': 'tipo_empregador',
                'CNPJ/CEI Empregador': 'cnpj_cei_empregador'
            }
            
            # Renomear colunas
            df = df.rename(columns=column_mapping)
            
            # Adicionar coluna de arquivo origem
            df['arquivo_origem'] = arquivo_origem
            
            logger.info(f"Colunas renomeadas e origem adicionada. Shape: {df.shape}")
            return df
            
        except Exception as e:
            logger.error(f"Erro ao processar dados: {e}")
            raise
    
    def load_csv_files(self, data_folder_path):
        """Carrega todos os arquivos CSV da pasta data para a tabela de staging"""
        try:
            # Buscar todos os arquivos CSV
            csv_files = glob.glob(os.path.join(data_folder_path, "*.csv"))
            
            if not csv_files:
                logger.warning(f"Nenhum arquivo CSV encontrado em {data_folder_path}")
                return
            
            logger.info(f"Encontrados {len(csv_files)} arquivos CSV para processar")
            
            total_records = 0
            
            for csv_file in csv_files:
                try:
                    arquivo_nome = os.path.basename(csv_file)
                    logger.info(f"Processando arquivo: {arquivo_nome}")
                    
                    # Ler CSV com encoding adequado
                    df = pd.read_csv(csv_file, sep=';', encoding='latin-1', low_memory=False)
                    
                    logger.info(f"Arquivo {arquivo_nome} carregado com {len(df)} registros")
                    
                    # Limpar e normalizar dados
                    df_clean = self.clean_and_normalize_data(df, arquivo_nome)
                    
                    # Carregar dados na tabela
                    df_clean.to_sql(
                        name='acidente_trabalho',
                        con=self.engine,
                        schema='schema_staging',
                        if_exists='append',
                        index=False,
                        method='multi',
                        chunksize=1000
                    )
                    
                    total_records += len(df_clean)
                    logger.info(f"Arquivo {arquivo_nome} carregado com sucesso! {len(df_clean)} registros inseridos")
                    
                except Exception as e:
                    logger.error(f"Erro ao processar arquivo {csv_file}: {e}")
                    continue
            
            logger.info(f"Carga concluída! Total de registros inseridos: {total_records}")
            
        except Exception as e:
            logger.error(f"Erro ao carregar arquivos CSV: {e}")
            raise
    
    def get_table_statistics(self):
        """Retorna estatísticas da tabela de staging"""
        try:
            with psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                client_encoding='latin1'
            ) as conn:
                cursor = conn.cursor(cursor_factory=RealDictCursor)
                
                # Estatísticas gerais
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_registros,
                        COUNT(DISTINCT arquivo_origem) as total_arquivos,
                        MIN(data_acidente) as data_acidente_min,
                        MAX(data_acidente) as data_acidente_max,
                        COUNT(DISTINCT sexo) as tipos_sexo,
                        COUNT(DISTINCT tipo_acidente) as tipos_acidente
                    FROM schema_staging.acidente_trabalho
                """)
                
                stats = cursor.fetchone()
                
                logger.info("=== ESTATÍSTICAS DA TABELA STAGING ===")
                logger.info(f"Total de registros: {stats['total_registros']}")
                logger.info(f"Total de arquivos processados: {stats['total_arquivos']}")
                logger.info(f"Data mais antiga: {stats['data_acidente_min']}")
                logger.info(f"Data mais recente: {stats['data_acidente_max']}")
                logger.info(f"Tipos de sexo distintos: {stats['tipos_sexo']}")
                logger.info(f"Tipos de acidente distintos: {stats['tipos_acidente']}")
                
                # Distribuição por arquivo
                cursor.execute("""
                    SELECT 
                        arquivo_origem,
                        COUNT(*) as registros
                    FROM schema_staging.acidente_trabalho
                    GROUP BY arquivo_origem
                    ORDER BY arquivo_origem
                """)
                
                logger.info("\n=== DISTRIBUIÇÃO POR ARQUIVO ===")
                for row in cursor.fetchall():
                    logger.info(f"{row['arquivo_origem']}: {row['registros']} registros")
                
        except Exception as e:
            logger.error(f"Erro ao obter estatísticas: {e}")
            raise