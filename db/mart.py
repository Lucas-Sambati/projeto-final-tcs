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
    
    # EXEMPLO
    def execute_query(self, query, params=None):
        """
        Executa uma query SQL e retorna os resultados
        
        Args:
            query (str): Query SQL a ser executada
            params (tuple, optional): Parâmetros para a query
            
        Returns:
            list: Lista de resultados da query
        """
        cursor = self.get_connection()
        try:
            cursor.execute(query, params)
            results = cursor.fetchall()
            return results
        except Exception as e:
            logger.error(f"Erro ao executar query: {e}")
            raise
        finally:
            self.close_connection(cursor)
