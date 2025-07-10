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
        
    def get_connection(self):
        """
        Estabelece conexão com o banco de dados e retorna o cursor
        
        Returns:
            psycopg2.extensions.cursor: Cursor para executar comandos SQL
        """
        try:
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,  # Usando o database da instância, não 'postgres'
                user=self.user,
                password=self.password,
                client_encoding='latin1'
            )
            
            conn.autocommit = True
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            logger.info(f"Conexão estabelecida com sucesso no banco {self.database}")
            return cursor
            
        except psycopg2.Error as e:
            logger.error(f"Erro ao conectar ao banco de dados: {e}")
            raise
    
    def close_connection(self, cursor):
        """
        Fecha a conexão com o banco de dados
        
        Args:
            cursor: Cursor retornado pela função get_connection()
        """
        try:
            if cursor:
                cursor.close()
                cursor.connection.close()
                logger.info("Conexão fechada com sucesso")
        except Exception as e:
            logger.error(f"Erro ao fechar conexão: {e}")
    
    def create_core_table(self):
        """Cria a tabela de core para acidentes de trabalho"""
        try:
            cursor = self.get_connection()
            # SQL para criar a tabela de core
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS schema_core.acidente_trabalho (
                id SERIAL PRIMARY KEY,
                agente_causador_acidente VARCHAR,
                data_acidente DATE,
                cid_10_codigo BIGINT,
                cnae_empregador_codigo BIGINT,
                indica_obito_acidente VARCHAR,
                municipio_empregador VARCHAR,
                natureza_lesao VARCHAR,
                parte_corpo_atingida VARCHAR,
                sexo VARCHAR,
                tipo_acidente VARCHAR,
                uf_municipio_acidente VARCHAR,
                uf_municipio_empregador VARCHAR,
                data_nascimento DATE,
                data_emissao_cat DATE,
                cnpj_cei_empregador BIGINT,
                data_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            
            cursor.execute(create_table_sql)
            
            logger.info("Tabela schema_core.acidente_trabalho criada com sucesso!")
                
        except Exception as e:
            logger.error(f"Erro ao criar tabela de core: {e}")
            raise
        finally:
            self.close_connection(cursor)
    
    def clean_and_normalize_data(self, df):
        """
        Limpa e normaliza os dados vindos da staging
        
        Args:
            df (pd.DataFrame): DataFrame com os dados da staging
            
        Returns:
            pd.DataFrame: DataFrame limpo e normalizado
        """
        try:
            logger.info("Iniciando limpeza e normalização dos dados")
            
            # Fazer uma cópia para não modificar o original
            df_clean = df.copy()
            
            # Remover espaços em branco das colunas de texto
            text_columns = ['agente_causador_acidente', 'indica_obito_acidente', 
                          'municipio_empregador', 'natureza_lesao', 'parte_corpo_atingida',
                          'sexo', 'tipo_acidente', 'uf_municipio_acidente', 'uf_municipio_empregador']
            
            for col in text_columns:
                if col in df_clean.columns:
                    df_clean[col] = df_clean[col].astype(str).str.strip().str.upper()
                    # Substituir strings vazias por None
                    df_clean[col] = df_clean[col].replace('', None)
                    df_clean[col] = df_clean[col].replace('NAN', None)
                    df_clean[col] = df_clean[col].replace('NONE', None)
            
            # Tratar datas
            date_columns = ['data_acidente', 'data_nascimento', 'data_emissao_cat']
            for col in date_columns:
                if col in df_clean.columns:
                    df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce', dayfirst=True)
            
            # Tratar campos numéricos com verificação de limites
            numeric_columns = ['cid_10_codigo', 'cnae_empregador_codigo', 'cnpj_cei_empregador']
            for col in numeric_columns:
                if col in df_clean.columns:
                    # Converter para numérico, substituindo valores inválidos por None
                    df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
                    
                    # Verificar se há valores muito grandes para BIGINT (limite do PostgreSQL)
                    max_bigint = 9223372036854775807  # Limite máximo para BIGINT
                    min_bigint = -9223372036854775808  # Limite mínimo para BIGINT
                    
                    # Substituir valores fora do limite por None
                    df_clean.loc[df_clean[col] > max_bigint, col] = None
                    df_clean.loc[df_clean[col] < min_bigint, col] = None
                    
                    # Log de valores problemáticos para debug
                    invalid_count = df_clean[col].isna().sum()
                    if invalid_count > 0:
                        logger.warning(f"Campo {col}: {invalid_count} valores inválidos ou fora do limite convertidos para NULL")
            
            # Selecionar apenas as colunas que existem na tabela core
            core_columns = ['agente_causador_acidente', 'data_acidente', 'cid_10_codigo',
                          'cnae_empregador_codigo', 'indica_obito_acidente', 'municipio_empregador',
                          'natureza_lesao', 'parte_corpo_atingida', 'sexo', 'tipo_acidente',
                          'uf_municipio_acidente', 'uf_municipio_empregador', 'data_nascimento',
                          'data_emissao_cat', 'cnpj_cei_empregador']
            
            # Filtrar apenas as colunas que existem no DataFrame
            available_columns = [col for col in core_columns if col in df_clean.columns]
            df_clean = df_clean[available_columns]
            
            logger.info(f"Dados limpos e normalizados. Shape final: {df_clean.shape}")
            return df_clean
            
        except Exception as e:
            logger.error(f"Erro ao limpar e normalizar dados: {e}")
            raise
    
    def load_data_from_staging_to_core(self, batch_size=1000):
        """
        Carrega dados da tabela de staging, trata e insere na tabela core
        
        Args:
            batch_size (int): Tamanho do lote para processamento
        """
        try:
            logger.info("Iniciando carregamento de dados da staging para core")
            
            # Primeiro, verificar se há dados na staging
            cursor = self.get_connection()
            cursor.execute("SELECT COUNT(*) as count FROM schema_staging.acidente_trabalho")
            result = cursor.fetchone()
            total_records = result['count']
            
            if total_records == 0:
                logger.warning("Nenhum dado encontrado na tabela staging")
                return
            
            logger.info(f"Total de registros na staging: {total_records}")
            
            # Limpar tabela core antes de inserir novos dados
            cursor.execute("TRUNCATE TABLE schema_core.acidente_trabalho RESTART IDENTITY")
            logger.info("Tabela core limpa para nova carga")
            
            self.close_connection(cursor)
            
            # Processar dados em lotes
            offset = 0
            total_inserted = 0
            
            while offset < total_records:
                try:
                    # Carregar lote de dados da staging
                    query = f"""
                    SELECT agente_causador_acidente, data_acidente, cid_10_codigo,
                           cnae_empregador_codigo, indica_obito_acidente, municipio_empregador,
                           natureza_lesao, parte_corpo_atingida, sexo, tipo_acidente,
                           uf_municipio_acidente, uf_municipio_empregador, data_nascimento,
                           data_emissao_cat, cnpj_cei_empregador
                    FROM schema_staging.acidente_trabalho
                    ORDER BY id
                    LIMIT {batch_size} OFFSET {offset}
                    """
                    
                    df_batch = pd.read_sql(query, self.engine)
                    
                    if df_batch.empty:
                        break
                    
                    # Limpar e normalizar dados
                    df_clean = self.clean_and_normalize_data(df_batch)
                    
                    # Inserir na tabela core
                    df_clean.to_sql(
                        name='acidente_trabalho',
                        con=self.engine,
                        schema='schema_core',
                        if_exists='append',
                        index=False,
                        method='multi'
                    )
                    
                    total_inserted += len(df_clean)
                    offset += batch_size
                    
                    logger.info(f"Processado lote: {offset}/{total_records} registros. Inseridos: {total_inserted}")
                    
                except Exception as e:
                    logger.error(f"Erro ao processar lote offset {offset}: {e}")
                    # Log dos dados problemáticos para debug
                    if 'df_batch' in locals():
                        logger.error(f"Tipos de dados no lote: {df_batch.dtypes}")
                        for col in ['cid_10_codigo', 'cnae_empregador_codigo', 'cnpj_cei_empregador']:
                            if col in df_batch.columns:
                                max_val = pd.to_numeric(df_batch[col], errors='coerce').max()
                                min_val = pd.to_numeric(df_batch[col], errors='coerce').min()
                                logger.error(f"Campo {col}: min={min_val}, max={max_val}")
                    offset += batch_size
                    continue
            
            logger.info(f"Carregamento concluído! Total de registros inseridos na core: {total_inserted}")
            
        except Exception as e:
            logger.error(f"Erro ao carregar dados da staging para core: {e}")
            raise
