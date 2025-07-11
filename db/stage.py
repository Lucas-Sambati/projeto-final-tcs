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

class StageSetup:
    """Classe para configurar e gerenciar o Stage do Data Warehouse"""
    
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

    def create_stage_table_acidente(self):
        """Cria a tabela de stage para acidentes de trabalho"""
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
                # SQL para criar a tabela de stage
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS schema_stage.acidente_trabalho (
                    id SERIAL PRIMARY KEY,
                    agente_causador_acidente VARCHAR,
                    data_acidente VARCHAR,
                    cbo_codigo INT,
                    cbo_descricao VARCHAR,
                    cid_10_codigo VARCHAR,
                    cid_10_descricao VARCHAR,
                    cnae_empregador_codigo INT,
                    cnae_empregador_descricao VARCHAR,
                    emitente_cat VARCHAR,
                    especie_beneficio VARCHAR,
                    filiacao_segurado VARCHAR,
                    indica_obito_acidente VARCHAR,
                    municipio_empregador VARCHAR,
                    natureza_lesao VARCHAR,
                    origem_cadastramento_cat VARCHAR,
                    parte_corpo_atingida VARCHAR,
                    sexo VARCHAR,
                    tipo_acidente VARCHAR,
                    uf_municipio_acidente VARCHAR,
                    uf_municipio_empregador VARCHAR,
                    data_afastamento VARCHAR,
                    data_despacho_beneficio VARCHAR,
                    data_acidente_duplicada VARCHAR,
                    data_nascimento VARCHAR,
                    data_emissao_cat VARCHAR,
                    tipo_empregador VARCHAR,
                    cnpj_cei_empregador VARCHAR,
                    data_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
                
                cursor.execute(create_table_sql)
                
                conn.commit()
                logger.info("Tabela schema_stage.acidente_trabalho criada com sucesso!")
                
        except Exception as e:
            logger.error(f"Erro ao criar tabela de stage acidente_trabalho: {e}")
            raise
    
    def clean_and_normalize_data_acidente(self, df, arquivo_origem):
        """Apenas renomeia as colunas e adiciona origem do arquivo"""
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
            
            logger.info(f"Colunas renomeadas. Shape: {df.shape}")
            return df
            
        except Exception as e:
            logger.error(f"Erro ao processar dados: {e}")
            raise
    
    def load_csv_files_acidente(self, data_folder_path):
        """Carrega o arquivos CSV da pasta acidente para a tabela de stage"""
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
                    df_clean = self.clean_and_normalize_data_acidente(df, arquivo_nome)
                    
                    # Carregar dados na tabela
                    df_clean.to_sql(
                        name='acidente_trabalho',
                        con=self.engine,
                        schema='schema_stage',
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

    def create_stage_table_municipio(self):
        """Cria a tabela de stage para acidentes de trabalho"""
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
                # SQL para criar a tabela de stage
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS schema_stage.municipio (
                    id SERIAL PRIMARY KEY,
                    municipio_tom_codigo INT,
                    municipio_ibge_codigo INT,
                    municipio_tom_descricao VARCHAR,
                    municipio_ibge_descricao VARCHAR,
                    municipio_uf VARCHAR(2)
                );
                """
                
                cursor.execute(create_table_sql)
                
                conn.commit()
                logger.info("Tabela schema_stage.municipio criada com sucesso!")
                
        except Exception as e:
            logger.error(f"Erro ao criar tabela de stage municipio: {e}")
            raise

    def clean_and_normalize_data_municipio(self, df, arquivo_origem):
        """Apenas renomeia as colunas e adiciona origem do arquivo"""
        try:
            # Mapear colunas para nomes padronizados
            column_mapping = {
                'CÓDIGO DO MUNICÍPIO - TOM': 'municipio_tom_codigo',
                'CÓDIGO DO MUNICÍPIO - IBGE': 'municipio_ibge_codigo',
                'MUNICÍPIO - TOM': 'municipio_tom_descricao',
                'MUNICÍPIO - IBGE': 'municipio_ibge_descricao',
                'UF': 'municipio_uf',
            }
            
            # Renomear colunas
            df = df.rename(columns=column_mapping)
                        
            logger.info(f"Colunas renomeadas e origem adicionada. Shape: {df.shape}")
            return df
            
        except Exception as e:
            logger.error(f"Erro ao processar dados: {e}")
            raise
    
    def load_csv_file_municipio(self, data_folder_path):
        """Carrega o arquivo CSV municipio para sua tabela de stage"""
        try:
            # Busca o arquivo CSV
            csv_files = glob.glob(os.path.join(data_folder_path, "municipios.csv"))
            
            if not csv_files:
                logger.warning(f"Nenhum arquivo CSV encontrado em {data_folder_path}")
                return
            
            logger.info(f"Encontrado {len(csv_files)} arquivo CSV para processar")
            
            total_records = 0
            
            for csv_file in csv_files:
                try:
                    arquivo_nome = os.path.basename(csv_file)
                    logger.info(f"Processando arquivo: {arquivo_nome}")
                    
                    # Ler CSV com encoding adequado
                    df = pd.read_csv(csv_file, sep=';', encoding='latin-1', low_memory=False)
                    
                    logger.info(f"Arquivo {arquivo_nome} carregado com {len(df)} registros")
                    
                    # Limpar e normalizar dados
                    df_clean = self.clean_and_normalize_data_municipio(df, arquivo_nome)
                    
                    # Carregar dados na tabela
                    df_clean.to_sql(
                        name='municipio',
                        con=self.engine,
                        schema='schema_stage',
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

    def create_stage_table_agente_causador(self):
        """Cria a tabela de stage para agentes causadores"""
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
                # SQL para criar a tabela de stage
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS schema_stage.agente_causador (
                    agente_codigo INT PRIMARY KEY,
                    agente_descricao VARCHAR
                );
                """
                
                cursor.execute(create_table_sql)
                
                conn.commit()
                logger.info("Tabela schema_stage.agente_causador criada com sucesso!")
                
        except Exception as e:
            logger.error(f"Erro ao criar tabela de stage agente_causador: {e}")
            raise

    def clean_and_normalize_data_agente_causador(self, df):
        """Apenas renomeia as colunas"""
        try:
            # Mapear colunas para nomes padronizados
            column_mapping = {
                'codigo': 'agente_codigo',
                'descricao': 'agente_descricao'
            }
            
            # Renomear colunas
            df = df.rename(columns=column_mapping)
                        
            logger.info(f"Colunas renomeadas e origem adicionada. Shape: {df.shape}")
            return df
            
        except Exception as e:
            logger.error(f"Erro ao processar dados: {e}")
            raise
    
    def load_csv_file_agente_causador(self, data_folder_path):
        """Carrega o arquivo CSV agente_causador para sua tabela de stage"""
        try:
            # Busca o arquivo CSV
            csv_files = glob.glob(os.path.join(data_folder_path, "agente_causador.csv"))
            
            if not csv_files:
                logger.warning(f"Nenhum arquivo CSV encontrado em {data_folder_path}")
                return
            
            logger.info(f"Encontrado {len(csv_files)} arquivo CSV para processar")
            
            total_records = 0
            
            for csv_file in csv_files:
                try:
                    arquivo_nome = os.path.basename(csv_file)
                    logger.info(f"Processando arquivo: {arquivo_nome}")
                    
                    # Ler CSV com encoding adequado
                    df = pd.read_csv(csv_file, sep=',', encoding='latin-1', low_memory=False)
                    logger.info(f"Colunas originais do CSV: {df.columns.tolist()}")
                    
                    logger.info(f"Arquivo {arquivo_nome} carregado com {len(df)} registros")
                    
                    # Limpar e normalizar dados
                    df_clean = self.clean_and_normalize_data_agente_causador(df)
                    
                    # Carregar dados na tabela
                    df_clean.to_sql(
                        name='agente_causador',
                        con=self.engine,
                        schema='schema_stage',
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
    
    def create_stage_table_natureza_lesao(self):
        """Cria a tabela de stage para a natureza das lesões"""
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
                # SQL para criar a tabela de stage
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS schema_stage.natureza_lesao (
                    natureza_codigo INT PRIMARY KEY,
                    natureza_descricao VARCHAR
                );
                """
                
                cursor.execute(create_table_sql)
                
                conn.commit()
                logger.info("Tabela schema_stage.natureza_lesao criada com sucesso!")
                
        except Exception as e:
            logger.error(f"Erro ao criar tabela de stage natureza_lesao: {e}")
            raise

    def clean_and_normalize_data_natureza_lesao(self, df):
        """Apenas renomeia as colunas"""
        try:
            # Mapear colunas para nomes padronizados
            column_mapping = {
                'codigo': 'natureza_codigo',
                'descricao': 'natureza_descricao'
            }
            
            # Renomear colunas
            df = df.rename(columns=column_mapping)
                        
            logger.info(f"Colunas renomeadas e origem adicionada. Shape: {df.shape}")
            return df
            
        except Exception as e:
            logger.error(f"Erro ao processar dados: {e}")
            raise
    
    def load_csv_file_natureza_lesao(self, data_folder_path):
        """Carrega o arquivo CSV natureza_lesao para sua tabela de stage"""
        try:
            # Busca o arquivo CSV
            csv_files = glob.glob(os.path.join(data_folder_path, "natureza_lesao.csv"))
            
            if not csv_files:
                logger.warning(f"Nenhum arquivo CSV encontrado em {data_folder_path}")
                return
            
            logger.info(f"Encontrado {len(csv_files)} arquivo CSV para processar")
            
            total_records = 0
            
            for csv_file in csv_files:
                try:
                    arquivo_nome = os.path.basename(csv_file)
                    logger.info(f"Processando arquivo: {arquivo_nome}")
                    
                    # Ler CSV com encoding adequado
                    df = pd.read_csv(csv_file, sep=',', encoding='latin-1', low_memory=False)
                    logger.info(f"Colunas originais do CSV: {df.columns.tolist()}")
                    
                    logger.info(f"Arquivo {arquivo_nome} carregado com {len(df)} registros")
                    
                    # Limpar e normalizar dados
                    df_clean = self.clean_and_normalize_data_natureza_lesao(df)
                    
                    # Carregar dados na tabela
                    df_clean.to_sql(
                        name='natureza_lesao',
                        con=self.engine,
                        schema='schema_stage',
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

    def create_stage_table_cid10(self):
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
                
                # SQL para criar a tabela CID10
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS schema_stage.cid10 (
                    cid10_codigo TEXT PRIMARY KEY,
                    cid10_descricao TEXT
                );
                """
                
                cursor.execute(create_table_sql)        
                
                conn.commit()
                logger.info("Tabela schema_stage.cid10 criada com sucesso!")
                
        except Exception as e:
            logger.error(f"Erro ao criar tabela CID10: {e}")
            raise

    def load_csv_file_cid10(self, data_folder_path):
        """Carrega os dados do CSV CID10 na tabela CID10"""
        try:
            # Buscar o arquivo CSV
            csv_files = glob.glob(os.path.join(data_folder_path, "cid10.csv"))
            
            if not csv_files:
                logger.warning(f"Nenhum arquivo CSV encontrado em {data_folder_path}")
                return
            
            # Pegando o primeiro arquivo encontrado (caso haja mais de um)
            csv_file = csv_files[0]
            logger.info(f"Processando arquivo: {csv_file}")
            
            # Ler CSV com encoding adequado
            df = pd.read_csv(csv_file, sep=',', encoding='latin-1')
            logger.info(f"Arquivo CID10 carregado com {len(df)} registros")
            
            # Renomear colunas
            df = df.rename(columns={'SUBCAT': 'cid10_codigo', 'DESCRICAO': 'cid10_descricao'})
            
            # Carregar dados na tabela CID10
            df.to_sql(
                name='cid10',
                con=self.engine,
                schema='schema_stage',
                if_exists='append',
                index=False,
                method='multi',
                chunksize=1000
            )
            
            logger.info(f"Dados CID10 carregados com sucesso! {len(df)} registros inseridos")
            
        except Exception as e:
            logger.error(f"Erro ao carregar dados CID10: {e}")
            raise

    def create_stage_table_auxiliar(self):
        """Executa a criação das tabelas auxiliares"""
        self.create_stage_table_municipio()
        self.create_stage_table_agente_causador()
        self.create_stage_table_natureza_lesao()
        self.create_stage_table_cid10()
    
    def load_csv_files_auxiliar(self):
        """Carrega os arquivos CSV das tabelas auxiliares"""
        data_folder = os.path.join(os.path.dirname(__file__), '../data/auxiliar')

        self.load_csv_file_municipio(data_folder)
        self.load_csv_file_agente_causador(data_folder)
        self.load_csv_file_natureza_lesao(data_folder)
        self.load_csv_file_cid10(data_folder)