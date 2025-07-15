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
    
    def create_mart_view_fato_acidentes_mes_metricas(self):
        """Cria a view de mart para acidentes metricas"""
        try:
            cursor = self.get_connection()
            # SQL para criar a view de mart
            create_table_sql = """
            CREATE OR REPLACE VIEW schema_mart.v_fato_acidentes_mes_metricas AS(
            WITH acidentes AS (
                SELECT
                    mes,
                    indica_obito_acidente,
                    cid_10_codigo,
                    sexo,
                    cnae_empregador_codigo,
                    estado_empregador
                FROM schema_core.acidente_trabalho
            )
            SELECT
            mes,
			estado_empregador,
			sexo,
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
				AND a2.estado_empregador = a1.estado_empregador
				AND a2.sexo = a1.sexo
                GROUP BY a2.cid_10_codigo
                ORDER BY COUNT(*) DESC
                LIMIT 1
            ) AS cid_recorrente_cod,

            (
                SELECT c.cid10_descricao
                FROM acidentes a2
                JOIN schema_core.cid10 c ON a2.cid_10_codigo = c.cid10_codigo
                WHERE a2.mes = a1.mes 
				AND a2.estado_empregador = a1.estado_empregador
				AND a2.sexo = a1.sexo
                GROUP BY c.cid10_descricao
                ORDER BY COUNT(*) DESC
                LIMIT 1
            ) AS cid_recorrente_desc,

            (
                SELECT a2.sexo
                FROM acidentes a2
                WHERE a2.mes = a1.mes 
				AND a2.estado_empregador = a1.estado_empregador
				AND a2.sexo = a1.sexo
                GROUP BY a2.sexo
                ORDER BY COUNT(*) DESC
                LIMIT 1
            ) AS sexo_recorrente,

            (
                SELECT c.cnae_descricao
                FROM acidentes a2
                JOIN schema_core.cnae c ON a2.cnae_empregador_codigo = c.cnae_codigo
                WHERE a2.mes = a1.mes 
				AND a2.estado_empregador = a1.estado_empregador
				AND a2.sexo = a1.sexo
                GROUP BY c.cnae_descricao
                ORDER BY COUNT(*) DESC
                LIMIT 1
            ) AS cnae_recorrente

            FROM acidentes a1
            GROUP BY mes, estado_empregador, sexo
            );
            """
            
            cursor.execute(create_table_sql)
            
            logger.info("View schema_mart.v_fato_acidentes_mes_metricas criada com sucesso!")
                
        except Exception as e:
            logger.error(f"Erro ao criar tabela de mart: {e}")
            raise
        finally:
            self.close_connection(cursor)
    
    def create_mart_view_fato_acidentes_mes_estado(self):
        """Cria a view de mart para acidentes estado"""
        try:
            cursor = self.get_connection()
            # SQL para criar a view de mart
            create_table_sql = """
            CREATE OR REPLACE VIEW schema_mart.v_fato_acidentes_mes_estados AS(
                SELECT estado_empregador as estado, sexo, COUNT(*) as total_acidentes, mes  
            FROM schema_core.acidente_trabalho
            GROUP BY estado_empregador, mes, sexo
            ORDER BY total_acidentes DESC
            );
            """
            
            cursor.execute(create_table_sql)
            
            logger.info("View schema_mart.v_fato_acidentes_mes_estado criada com sucesso!")
                
        except Exception as e:
            logger.error(f"Erro ao criar tabela de mart: {e}")
            raise
        finally:
            self.close_connection(cursor)
    
    def create_mart_view_fato_acidentes_mes_setor(self):
        """Cria a view de mart para acidentes setor"""
        try:
            cursor = self.get_connection()
            # SQL para criar a view de mart
            create_table_sql = """
            CREATE VIEW schema_mart.v_fato_acidentes_mes_setor AS (
                SELECT 
                    mes, 
                    estado_empregador, 
                    sexo, 
                    cnae_empregador_codigo AS setor_id, 
                    natureza_lesao, 
                    tipo_acidente, 
                    COUNT(*) as total_acidentes
            FROM schema_core.acidente_trabalho
            GROUP BY mes, estado_empregador, sexo, setor_id, natureza_lesao, tipo_acidente
            );
            """
            
            cursor.execute(create_table_sql)
            
            logger.info("View schema_mart.v_fato_acidentes_setor criada com sucesso!")
                
        except Exception as e:
            logger.error(f"Erro ao criar tabela de mart: {e}")
            raise
        finally:
            self.close_connection(cursor)

    def create_mart_view_fato_acidentes_mes_localidade(self):
        """Cria a view de mart para acidentes localidade"""
        try:
            cursor = self.get_connection()
            # SQL para criar a view de mart
            create_table_sql = """
            CREATE VIEW schema_mart.v_fato_acidentes_mes_localidade AS (
                SELECT 
                    mes, 
                    estado_empregador, 
                    sexo, 
                    m.municipio_ibge_descricao AS municipio, 
                    COUNT(*) AS total_acidentes,
                    SUM(CASE WHEN indica_obito_acidente = 'SIM' THEN 1 ELSE 0 END) AS total_mortes
            FROM schema_core.acidente_trabalho a
            JOIN schema_core.municipio m
            ON a.municipio_empregador_codigo = m.municipio_ibge_codigo
            GROUP BY mes, estado_empregador, sexo, municipio
            ORDER BY total_acidentes DESC
            );
            """
            
            cursor.execute(create_table_sql)
            
            logger.info("View schema_mart.v_fato_acidentes_mes_localidade criada com sucesso!")
                
        except Exception as e:
            logger.error(f"Erro ao criar tabela de mart: {e}")
            raise
        finally:
            self.close_connection(cursor)
    
    def create_mart_view_dim_tempo(self):
        """Cria a view de mart para tempo"""
        try:
            cursor = self.get_connection()
            # SQL para criar a view de mart
            create_table_sql = """
            CREATE VIEW schema_mart.v_dim_tempo AS(
            SELECT DISTINCT
                mes
            FROM schema_core.acidente_trabalho
            WHERE data_acidente IS NOT NULL
            );
            """
            
            cursor.execute(create_table_sql)
            
            logger.info("View schema_mart.v_dim_tempo criada com sucesso!")
                
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
                estado_empregador AS estado
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
    
    def create_mart_view_dim_sexo(self):
        """Cria a view de mart para sexo"""
        try:
            cursor = self.get_connection()
            # SQL para criar a view de mart
            create_table_sql = """
            CREATE VIEW schema_mart.v_dim_sexo AS(
            SELECT DISTINCT
                sexo
            FROM schema_core.acidente_trabalho
            );
            """
            
            cursor.execute(create_table_sql)
            
            logger.info("View schema_mart.v_dim_sexo criada com sucesso!")
                
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
            SELECT DISTINCT
                c.cnae_codigo AS setor_id,
                c.cnae_descricao AS setor_descricao
            FROM schema_core.cnae c
            JOIN schema_core.acidente_trabalho a
            ON c.cnae_codigo = a.cnae_empregador_codigo
            );
            """
            
            cursor.execute(create_table_sql)
            
            logger.info("View schema_mart.v_dim_time criada com sucesso!")
                
        except Exception as e:
            logger.error(f"Erro ao criar tabela de mart: {e}")
            raise
        finally:
            self.close_connection(cursor)

    def create_mart_view_dim_municipio(self):
        """Cria a view de mart para municipio"""
        try:
            cursor = self.get_connection()
            # SQL para criar a view de mart
            create_table_sql = """
            CREATE VIEW schema_mart.v_dim_municipio AS(
            SELECT DISTINCT
                m.municipio_ibge_descricao AS muncipio
            FROM schema_core.municipio m
            JOIN schema_core.acidente_trabalho a
            ON a.municipio_empregador_codigo = m.municipio_ibge_codigo
            );
            """
            
            cursor.execute(create_table_sql)
            
            logger.info("View schema_mart.v_dim_time criada com sucesso!")
                
        except Exception as e:
            logger.error(f"Erro ao criar tabela de mart: {e}")
            raise
        finally:
            self.close_connection(cursor)

    def create_mart_views(self):
        """Executa a criação das views no mart"""
#        self.create_mart_view_fato_acidentes_mes_metricas()
#        self.create_mart_view_fato_acidentes_mes_estado()
#        self.create_mart_view_fato_acidentes_mes_setor()
#        self.create_mart_view_dim_tempo()
#        self.create_mart_view_dim_estado()
#        self.create_mart_view_dim_sexo()
#        self.create_mart_view_dim_setor()
        self.create_mart_view_fato_acidentes_mes_localidade()
        self.create_mart_view_dim_municipio()
        logger.info("Todas as views de mart foram criadas com sucesso!")