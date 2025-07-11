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
                
                schemas = ['schema_stage', 'schema_core', 'schema_mart']
                
                for schema in schemas:
                    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
                    logger.info(f"Schema {schema} criado/verificado com sucesso")
                
                conn.commit()
                logger.info("Todos os schemas foram criados com sucesso!")
                
        except Exception as e:
            logger.error(f"Erro ao criar schemas: {e}")
            raise