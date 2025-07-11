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

class CoreSetup:
    """Classe para configurar e gerenciar o Core do Data Warehouse"""
    
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
                database=self.database,  
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
    
    def create_core_table_acidente(self):
        """Cria a tabela de core para acidentes de trabalho"""
        try:
            cursor = self.get_connection()
            # SQL para criar a tabela de core
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS schema_core.acidente_trabalho (
                id SERIAL PRIMARY KEY,
                agente_causador_acidente VARCHAR,
                data_acidente DATE,
                cid_10_codigo VARCHAR(4),
                cnae_empregador_codigo INT,
                indica_obito_acidente VARCHAR,
                municipio_empregador INT,
                natureza_lesao VARCHAR,
                parte_corpo_atingida VARCHAR,
                sexo VARCHAR,
                tipo_acidente VARCHAR,
                uf_municipio_acidente VARCHAR,
                uf_municipio_empregador VARCHAR,
                data_nascimento DATE,
                data_emissao_cat DATE,
                data_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (municipio_empregador) REFERENCES schema_core.municipio (municipio_ibge_codigo)
            );
            """
            
            cursor.execute(create_table_sql)
            
            logger.info("Tabela schema_core.acidente_trabalho criada com sucesso!")
                
        except Exception as e:
            logger.error(f"Erro ao criar tabela de core: {e}")
            raise
        finally:
            self.close_connection(cursor)

    def create_core_table_cid10(self):
        """Cria a tabela de core para CID10"""
        try:
            cursor = self.get_connection()
            # SQL para criar a tabela de core
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS schema_core.cid10 (
                cid10_codigo TEXT PRIMARY KEY,
                cid10_descricao TEXT
            );
            """
            
            cursor.execute(create_table_sql)
            
            logger.info("Tabela schema_core.cid10 criada com sucesso!")
                    
        except Exception as e:
            logger.error(f"Erro ao criar tabela de core: {e}")
            raise
        finally:
            self.close_connection(cursor)

    
    def clean_and_normalize_data_acidente(self, df):
        """
        Limpa e normaliza os dados vindos da stage
        
        Args:
            df (pd.DataFrame): DataFrame com os dados da stage
            
        Returns:
            pd.DataFrame: DataFrame limpo e normalizado
        """
        try:
            logger.info("Iniciando limpeza e normalização dos dados")
            
            # Fazer uma cópia para não modificar o original
            df_clean = df.copy()
            
            # Remover espaços em branco das colunas de texto
            text_columns = ['agente_causador_acidente', 'cid_10_codigo', 'indica_obito_acidente', 
                          'municipio_empregador', 'natureza_lesao', 'parte_corpo_atingida',
                          'sexo', 'tipo_acidente', 'uf_municipio_acidente', 'uf_municipio_empregador']
            
            for col in text_columns:
                if col in df_clean.columns:
                    df_clean[col] = df_clean[col].astype(str).str.strip().str.upper()
                    # Substituir strings vazias por None
                    df_clean[col] = df_clean[col].replace('', 'DESCONHECIDO')
                    df_clean[col] = df_clean[col].replace('NAN', 'DESCONHECIDO')
                    df_clean[col] = df_clean[col].replace('NONE', 'DESCONHECIDO')
                    df_clean[col] = df_clean[col].replace('{Ñ CLASS}', 'DESCONHECIDO')
            
            # Tratar datas
            date_columns = ['data_acidente', 'data_nascimento', 'data_emissao_cat']
            for col in date_columns:
                if col in df_clean.columns:
                    df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce', dayfirst=True)
            
            # Tratar campos numéricos com verificação de limites
            numeric_columns = ['cnae_empregador_codigo']
            for col in numeric_columns:
                if col in df_clean.columns:
                    # Substituir numéricos vazios por None
                    df_clean[col] = df_clean[col].replace('', None)
                    df_clean[col] = df_clean[col].replace('NAN', None)
                    df_clean[col] = df_clean[col].replace('NONE', None)

            # Tratar coluna municipio_empregador
            df_clean['municipio_empregador'] = df_clean['municipio_empregador'].str.split('-').str[0].str.strip()
            df_clean['municipio_empregador'] = df_clean['municipio_empregador'].replace('DESCONHECIDO', 0)
            df_clean['municipio_empregador'] = df_clean['municipio_empregador'].astype(int)

            # Selecionar apenas as colunas que existem na tabela core
            core_columns = ['agente_causador_acidente', 'data_acidente', 'cid_10_codigo',
                          'cnae_empregador_codigo', 'indica_obito_acidente', 'municipio_empregador',
                          'natureza_lesao', 'parte_corpo_atingida', 'sexo', 'tipo_acidente',
                          'uf_municipio_acidente', 'uf_municipio_empregador', 'data_nascimento',
                          'data_emissao_cat']
            
            # Filtrar apenas as colunas que existem no DataFrame
            available_columns = [col for col in core_columns if col in df_clean.columns]
            df_clean = df_clean[available_columns]
            
            logger.info(f"Dados limpos e normalizados. Shape final: {df_clean.shape}")
            return df_clean
            
        except Exception as e:
            logger.error(f"Erro ao limpar e normalizar dados: {e}")
            raise

    def clean_and_normalize_data_cid10(self, df):
        """
        Limpa e normaliza os dados da tabela `cid10` da stage
        
        Args:
            df (pd.DataFrame): DataFrame com os dados da stage
        
        Returns:
            pd.DataFrame: DataFrame limpo e normalizado
        """
        try:
            logger.info("Iniciando limpeza e normalização dos dados")
            
            # Fazer uma cópia para não modificar o original
            df_clean = df.copy()
            
            # Remover espaços em branco das colunas de texto
            text_columns = ['cid10_descricao']
            
            for col in text_columns:
                if col in df_clean.columns:
                    df_clean[col] = df_clean[col].astype(str).str.strip().str.upper()
                    # Substituir strings vazias por None
                    df_clean[col] = df_clean[col].replace('', 'DESCONHECIDO')
                    df_clean[col] = df_clean[col].replace('NAN', 'DESCONHECIDO')
                    df_clean[col] = df_clean[col].replace('NONE', 'DESCONHECIDO')
                    df_clean[col] = df_clean[col].replace('{Ñ CLASS}', 'DESCONHECIDO')

            logger.info(f"Dados limpos e normalizados. Shape final: {df_clean.shape}")
            return df_clean
            
        except Exception as e:
            logger.error(f"Erro ao limpar e normalizar dados: {e}")
            raise

    
    def load_data_from_stage_to_core_acidente(self, batch_size=1000):
        """
        Carrega dados da tabela de stage, trata e insere na tabela core
        
        Args:
            batch_size (int): Tamanho do lote para processamento
        """
        try:
            logger.info("Iniciando carregamento de dados da staging para core")
            
            # Primeiro, verificar se há dados na stage
            cursor = self.get_connection()
            cursor.execute("SELECT COUNT(*) as count FROM schema_stage.acidente_trabalho")
            result = cursor.fetchone()
            total_records = result['count']
            
            if total_records == 0:
                logger.warning("Nenhum dado encontrado na tabela stage principal")
                return
            
            logger.info(f"Total de registros na tabela stage principal: {total_records}")
            
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
                           data_emissao_cat
                    FROM schema_stage.acidente_trabalho
                    ORDER BY id
                    LIMIT {batch_size} OFFSET {offset}
                    """
                    
                    df_batch = pd.read_sql(query, self.engine)
                    
                    if df_batch.empty:
                        break
                    
                    # Limpar e normalizar dados
                    df_clean = self.clean_and_normalize_data_acidente(df_batch)
                    
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
                    raise
            
            logger.info(f"Carregamento concluído! Total de registros inseridos na core: {total_inserted}")
            
        except Exception as e:
            logger.error(f"Erro ao carregar dados da staging para core: {e}")
            raise
        finally:
            self.close_connection(cursor)
    
    def create_core_table_municipio(self):
        """Cria a tabela de core para municipios"""
        try:
            cursor = self.get_connection()
            # SQL para criar a tabela de core
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS schema_core.municipio (
                municipio_ibge_codigo INT PRIMARY KEY,
                municipio_ibge_descricao VARCHAR
            );
            """
            
            cursor.execute(create_table_sql)
            
            logger.info("Tabela schema_core.municipio criada com sucesso!")
                
        except Exception as e:
            logger.error(f"Erro ao criar tabela de core: {e}")
            raise
        finally:
            self.close_connection(cursor)
    
    def clean_and_normalize_data_municipio(self, df):
        """
        Limpa e normaliza os dados vindos da stage
        
        Args:
            df (pd.DataFrame): DataFrame com os dados da stage
            
        Returns:
            pd.DataFrame: DataFrame limpo e normalizado
        """
        try:
            logger.info("Iniciando limpeza e normalização dos dados")
            
            # Fazer uma cópia para não modificar o original
            df_clean = df.copy()
            
            # Remover espaços em branco das colunas de texto
            text_columns = ['municipio_ibge_descricao']
            
            for col in text_columns:
                if col in df_clean.columns:
                    df_clean[col] = df_clean[col].astype(str).str.strip().str.upper()
                    # Substituir strings vazias por None
                    df_clean[col] = df_clean[col].replace('', 'DESCONHECIDO')
                    df_clean[col] = df_clean[col].replace('NAN', 'DESCONHECIDO')
                    df_clean[col] = df_clean[col].replace('NONE', 'DESCONHECIDO')
                    df_clean[col] = df_clean[col].replace('{Ñ CLASS}', 'DESCONHECIDO')

            # Tratar coluna municipio_ibge_codigo
            df_clean['municipio_ibge_codigo'] = df_clean['municipio_ibge_codigo'].astype(str).str[:-1]
            df_clean['municipio_ibge_codigo'] = df_clean['municipio_ibge_codigo'].str.strip()
            df_clean = df_clean[df_clean['municipio_ibge_codigo'] != '']
            df_clean['municipio_ibge_codigo'] = df_clean['municipio_ibge_codigo'].astype(int)


            # Selecionar apenas as colunas que existem na tabela core
            core_columns = ['municipio_ibge_codigo', 'municipio_ibge_descricao']
            
            # Filtrar apenas as colunas que existem no DataFrame
            available_columns = [col for col in core_columns if col in df_clean.columns]
            df_clean = df_clean[available_columns]

            # Remove o registro 0 - 'Exterior'
            df_clean = df_clean[df_clean['municipio_ibge_codigo'] != 0]
            
            logger.info(f"Dados limpos e normalizados. Shape final: {df_clean.shape}")
            return df_clean
            
        except Exception as e:
            logger.error(f"Erro ao limpar e normalizar dados: {e}")
            raise
    
    def load_data_from_stage_to_core_municipio(self, batch_size=1000):
        """
        Carrega dados da tabela de stage, trata e insere na tabela core
        
        Args:
            batch_size (int): Tamanho do lote para processamento
        """
        try:
            logger.info("Iniciando carregamento de dados da stage para core")
            
            # Primeiro, verificar se há dados na stage
            cursor = self.get_connection()
            cursor.execute("SELECT COUNT(*) as count FROM schema_stage.municipio")
            result = cursor.fetchone()
            total_records = result['count']
            
            if total_records == 0:
                logger.warning("Nenhum dado encontrado na tabela stage municipio")
                return
            
            logger.info(f"Total de registros na tabela stage municipio: {total_records}")
            
            self.close_connection(cursor)

            # Adiciona código de desconhecido
            df_0 = pd.DataFrame({
                'municipio_ibge_codigo': [0],
                'municipio_ibge_descricao': ['DESCONHECIDO']
            })
            df_0.to_sql(
                name='municipio',
                con=self.engine,
                schema='schema_core',
                if_exists='append',
                index=False,
                method='multi'
            )
            
            # Processar dados em lotes
            offset = 0
            total_inserted = 0
            
            while offset < total_records:
                try:
                    # Carregar lote de dados da staging
                    query = f"""
                    SELECT municipio_ibge_codigo, municipio_ibge_descricao
                    FROM schema_stage.municipio
                    ORDER BY id
                    LIMIT {batch_size} OFFSET {offset}
                    """
                    
                    df_batch = pd.read_sql(query, self.engine)
                    
                    if df_batch.empty:
                        break
                    
                    # Limpar e normalizar dados
                    df_clean = self.clean_and_normalize_data_municipio(df_batch)
                    
                    # Inserir na tabela core
                    df_clean.to_sql(
                        name='municipio',
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
                    raise
            
            logger.info(f"Carregamento concluído! Total de registros inseridos na core municipio: {total_inserted}")
            
        except Exception as e:
            logger.error(f"Erro ao carregar dados da staging para core: {e}")
            raise
        finally:
            self.close_connection(cursor)
    
    def load_data_from_stage_to_core_cid10(self, batch_size=1000):
        """
        Carrega dados da tabela de stage (cid10), trata e insere na tabela core (cid10)
        
        Args:
            batch_size (int): Tamanho do lote para processamento
        """
        try:
            logger.info("Iniciando carregamento de dados da staging para core")
            
            # Verificar se há dados na stage
            cursor = self.get_connection()
            cursor.execute("SELECT COUNT(*) as count FROM schema_stage.cid10")
            result = cursor.fetchone()
            total_records = result['count']
            
            if total_records == 0:
                logger.warning("Nenhum dado encontrado na tabela stage cid10")
                return
            
            logger.info(f"Total de registros na tabela stage cid10: {total_records}")
            
            self.close_connection(cursor)
            
            # Processar dados em lotes
            offset = 0
            total_inserted = 0
            
            while offset < total_records:
                try:
                    # Carregar lote de dados da staging
                    query = f"""
                    SELECT cid10_codigo, cid10_descricao
                    FROM schema_stage.cid10
                    ORDER BY cid10_codigo
                    LIMIT {batch_size} OFFSET {offset}
                    """
                    
                    df_batch = pd.read_sql(query, self.engine)
                    
                    if df_batch.empty:
                        break
                    
                    # Limpar e normalizar dados
                    df_clean = self.clean_and_normalize_data_cid10(df_batch)
                    
                    # Inserir na tabela core
                    df_clean.to_sql(
                        name='cid10',
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
                    raise
            
            logger.info(f"Carregamento concluído! Total de registros inseridos na core cid10: {total_inserted}")
            
        except Exception as e:
            logger.error(f"Erro ao carregar dados da staging para core cid10: {e}")
            raise
        finally:
            self.close_connection(cursor)


    def create_core_table_auxiliar(self):
        """Executa a criação das tabelas auxiliares no core"""
        self.create_core_table_municipio()
        self.create_core_table_cid10()
    
    def load_data_from_stage_to_core_auxiliar(self):
        """Carrega os dados das tabelas auxiliares do stage para o core"""
        self.load_data_from_stage_to_core_municipio()
        self.load_data_from_stage_to_core_cid10()