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

class MartSetup:
    """Classe para configurar e gerenciar a Mart do Data Warehouse"""
    
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
    
    def create_mart_view_fato_acidentes_mes_setor(self):
        """Cria a view de mart para acidentes por mes e setor"""
        try:
            cursor = self.get_connection()
            # SQL para criar a view de mart
            create_table_sql = """
            CREATE VIEW schema_mart.v_fato_acidentes_mes_setor AS(
            SELECT
                EXTRACT(MONTH FROM data_acidente) AS mes,
                cnae_empregador_codigo AS setor_id,
                COUNT(*) AS total_acidentes,
                SUM(CASE WHEN indica_obito_acidente = 'Sim' THEN 1 ELSE 0 END) AS total_fatais
            FROM schema_core.acidente_trabalho
            GROUP BY mes, setor_id
            );
            """
            
            cursor.execute(create_table_sql)
            
            logger.info("View schema_mart.v_fato_acidentes_mes_setor criada com sucesso!")
                
        except Exception as e:
            logger.error(f"Erro ao criar tabela de mart: {e}")
            raise
        finally:
            self.close_connection(cursor)

    def create_mart_view_fato_acidentes_mes_estado(self):
        """Cria a view de mart para acidentes por mes e estado"""
        try:
            cursor = self.get_connection()
            # SQL para criar a view de mart
            create_table_sql = """
            CREATE VIEW schema_mart.v_fato_acidentes_mes_estado AS(
            SELECT
                EXTRACT(MONTH FROM data_acidente) AS mes,
                estado_acidente AS uf,
                COUNT(*) AS total_acidentes,
                SUM(CASE WHEN indica_obito_acidente = 'Sim' THEN 1 ELSE 0 END) AS total_fatais
            FROM schema_core.acidente_trabalho
            GROUP BY mes, uf
            );
            """
            
            cursor.execute(create_table_sql)
            
            logger.info("View schema_mart.v_fato_acidentes_mes_estado criada com sucesso!")
                
        except Exception as e:
            logger.error(f"Erro ao criar tabela de mart: {e}")
            raise
        finally:
            self.close_connection(cursor)

    def create_mart_view_fato_acidentes_top_agentes(self):
        """Cria a view de mart para acidentes top agentes causadores"""
        try:
            cursor = self.get_connection()
            # SQL para criar a view de mart
            create_table_sql = """
            CREATE VIEW schema_mart.v_fato_acidentes_top_agentes AS(
            SELECT
                agente_causador_acidente AS agente_descricao,
                EXTRACT(MONTH FROM data_acidente) AS mes,
                COUNT(*) AS total_acidentes
            FROM schema_core.acidente_trabalho
            GROUP BY agente_descricao, mes
            ORDER BY total_acidentes DESC
            );
            """
            
            cursor.execute(create_table_sql)
            
            logger.info("View schema_mart.v_fato_acidentes_top_agentes criada com sucesso!")
                
        except Exception as e:
            logger.error(f"Erro ao criar tabela de mart: {e}")
            raise
        finally:
            self.close_connection(cursor)
    
    def create_mart_view_fato_acidentes_distribuicao_lesao(self):
        """Cria a view de mart para acidentes distribuição de lesão"""
        try:
            cursor = self.get_connection()
            # SQL para criar a view de mart
            create_table_sql = """
            CREATE VIEW schema_mart.v_fato_acidentes_distribuicao_lesao AS(
            SELECT
                natureza_lesao AS lesao_descricao,
                EXTRACT(MONTH FROM data_acidente) AS mes,
                COUNT(*) AS total_acidentes
            FROM schema_core.acidente_trabalho
            GROUP BY lesao_descricao, mes
            );
            """
            
            cursor.execute(create_table_sql)
            
            logger.info("View schema_mart.v_fato_acidentes_distribuicao_lesao criada com sucesso!")
                
        except Exception as e:
            logger.error(f"Erro ao criar tabela de mart: {e}")
            raise
        finally:
            self.close_connection(cursor)
    
    def create_mart_view_fato_acidentes_metricas(self):
        """Cria a view de mart para acidentes metricas"""
        try:
            cursor = self.get_connection()
            # SQL para criar a view de mart
            create_table_sql = """
            CREATE OR REPLACE VIEW schema_mart.v_fato_acidentes_metricas AS(
            WITH acidentes AS (
            SELECT
                EXTRACT(MONTH FROM data_acidente)::INT AS mes,
                indica_obito_acidente,
                cid_10_codigo,
                sexo,
                cnae_empregador_codigo
            FROM schema_core.acidente_trabalho
            )
            SELECT
            mes,
            COUNT(*) AS total_acidentes,
            ROUND(
                SUM(CASE WHEN indica_obito_acidente = 'SIM' THEN 1 ELSE 0 END)::NUMERIC
                /
                COUNT(*)::NUMERIC,
                4
            ) AS porcentagem_fatais,

            (
                SELECT a2.cid_10_codigo
                FROM acidentes a2
                WHERE a2.mes = a1.mes
                GROUP BY a2.cid_10_codigo
                ORDER BY COUNT(*) DESC
                LIMIT 1
            ) AS cid_recorrente_cod,

            (
                SELECT c.cid10_descricao
                FROM acidentes a2
                JOIN schema_core.cid10 c ON a2.cid_10_codigo = c.cid10_codigo
                WHERE a2.mes = a1.mes
                GROUP BY c.cid10_descricao
                ORDER BY COUNT(*) DESC
                LIMIT 1
            ) AS cid_recorrente_desc,

            (
                SELECT a2.sexo
                FROM acidentes a2
                WHERE a2.mes = a1.mes
                GROUP BY a2.sexo
                ORDER BY COUNT(*) DESC
                LIMIT 1
            ) AS sexo_recorrente,

            (
                SELECT c.cnae_descricao
                FROM acidentes a2
                JOIN schema_core.cnae c ON a2.cnae_empregador_codigo = c.cnae_codigo
                WHERE a2.mes = a1.mes
                GROUP BY c.cnae_descricao
                ORDER BY COUNT(*) DESC
                LIMIT 1
            ) AS cnae_recorrente

            FROM acidentes a1
            GROUP BY mes
            );
            """
            
            cursor.execute(create_table_sql)
            
            logger.info("View schema_mart.v_fato_acidentes_metricas criada com sucesso!")
                
        except Exception as e:
            logger.error(f"Erro ao criar tabela de mart: {e}")
            raise
        finally:
            self.close_connection(cursor)
    
    def create_mart_view_dim_time(self):
        """Cria a view de mart para tempo"""
        try:
            cursor = self.get_connection()
            # SQL para criar a view de mart
            create_table_sql = """
            CREATE VIEW schema_mart.v_dim_time AS(
            SELECT DISTINCT
                EXTRACT(MONTH FROM data_acidente)::INT AS month
            FROM schema_core.acidente_trabalho
            WHERE data_acidente IS NOT NULL
            );
            """
            
            cursor.execute(create_table_sql)
            
            logger.info("View schema_mart.v_dim_time criada com sucesso!")
                
        except Exception as e:
            logger.error(f"Erro ao criar tabela de mart: {e}")
            raise
        finally:
            self.close_connection(cursor)

    def create_mart_view_dim_setor(self):
        """Cria a view de mart para setor"""
        try:
            cursor = self.get_connection()
            # SQL para criar a view de mart
            create_table_sql = """
            CREATE VIEW schema_mart.v_dim_setor AS(
            SELECT
                cnae_codigo AS setor_id,
                cnae_descricao AS setor_descricao
            FROM schema_core.cnae
            );
            """
            
            cursor.execute(create_table_sql)
            
            logger.info("View schema_mart.v_dim_time criada com sucesso!")
                
        except Exception as e:
            logger.error(f"Erro ao criar tabela de mart: {e}")
            raise
        finally:
            self.close_connection(cursor)

    def create_mart_view_dim_lesao(self):
        """Cria a view de mart para lesao"""
        try:
            cursor = self.get_connection()
            # SQL para criar a view de mart
            create_table_sql = """
            CREATE VIEW schema_mart.v_dim_lesao AS(
            SELECT DISTINCT
                natureza_lesao AS lesao_descricao
            FROM schema_core.acidente_trabalho
            WHERE natureza_lesao IS NOT NULL
            );
            """
            
            cursor.execute(create_table_sql)
            
            logger.info("View schema_mart.v_dim_lesao criada com sucesso!")
                
        except Exception as e:
            logger.error(f"Erro ao criar tabela de mart: {e}")
            raise
        finally:
            self.close_connection(cursor)

    def create_mart_view_dim_agente(self):
        """Cria a view de mart para agente"""
        try:
            cursor = self.get_connection()
            # SQL para criar a view de mart
            create_table_sql = """
            CREATE VIEW schema_mart.v_dim_agente AS(
            SELECT DISTINCT
                agente_causador_acidente AS agente_descricao
            FROM schema_core.acidente_trabalho
            WHERE agente_causador_acidente IS NOT NULL
            );
            """
            
            cursor.execute(create_table_sql)
            
            logger.info("View schema_mart.v_dim_agente criada com sucesso!")
                
        except Exception as e:
            logger.error(f"Erro ao criar tabela de mart: {e}")
            raise
        finally:
            self.close_connection(cursor)

    def create_mart_view_dim_estado(self):
        """Cria a view de mart para estado"""
        try:
            cursor = self.get_connection()
            # SQL para criar a view de mart
            create_table_sql = """
            CREATE VIEW schema_mart.v_dim_estado AS(
            SELECT DISTINCT
                estado_acidente AS uf
            FROM schema_core.acidente_trabalho
            );
            """
            
            cursor.execute(create_table_sql)
            
            logger.info("View schema_mart.v_dim_estado criada com sucesso!")
                
        except Exception as e:
            logger.error(f"Erro ao criar tabela de mart: {e}")
            raise
        finally:
            self.close_connection(cursor)

    def create_mart_views(self):
        """Executa a criação das views no mart"""
        self.create_mart_view_fato_acidentes_mes_setor()
        self.create_mart_view_fato_acidentes_mes_estado()
        self.create_mart_view_fato_acidentes_top_agentes()
        self.create_mart_view_fato_acidentes_distribuicao_lesao()
        self.create_mart_view_dim_time()
        self.create_mart_view_dim_setor()
        self.create_mart_view_dim_lesao()
        self.create_mart_view_dim_agente()
        self.create_mart_view_dim_estado()
        self.create_mart_view_fato_acidentes_metricas()
        logger.info("Todas as views de mart foram criadas com sucesso!")