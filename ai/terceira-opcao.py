#import pandas as pd
#import psycopg2
#from psycopg2.extras import RealDictCursor
#from psycopg2.extras import execute_batch
#import os
#import glob
#from sqlalchemy import create_engine
#import logging
#from datetime import datetime
#import difflib
#import ollama
#
## Configuração de logging
#logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
#logger = logging.getLogger(__name__)
#
#class IASetup:
#    """Classe para configurar e gerenciar a comunicação com a IA"""
#
#    def __init__(self, 
#                 host='localhost', 
#                 port=5432, 
#                 database='acidente_trabalho_dw', 
#                 user='postgres', 
#                 password='postgres'):
#        """
#        Inicializa a conexão com o banco de dados PostgreSQL
#        
#        Args:
#            host (str): Host do PostgreSQL
#            port (int): Porta do PostgreSQL
#            database (str): Nome do banco de dados
#            user (str): Usuário do PostgreSQL
#            password (str): Senha do PostgreSQL
#        """
#        self.host = host
#        self.port = port
#        self.database = database
#        self.user = user
#        self.password = password
#        
#        # String de conexão para psycopg2
#        self.conn_string = f"postgresql://{user}:{password}@{host}:{port}/{database}?client_encoding=latin1"
#        
#        # Engine do SQLAlchemy para operações com pandas
#        self.engine = create_engine(self.conn_string, echo=False)
#
#    def get_connection(self):
#        """
#        Estabelece conexão com o banco de dados e retorna o cursor
#        
#        Returns:
#            psycopg2.extensions.cursor: Cursor para executar comandos SQL
#        """
#        try:
#            conn = psycopg2.connect(
#                host=self.host,
#                port=self.port,
#                database=self.database,  
#                user=self.user,
#                password=self.password,
#                client_encoding='latin1'
#            )
#            
#            conn.autocommit = True
#            cursor = conn.cursor(cursor_factory=RealDictCursor)
#            logger.info(f"Conexão estabelecida com sucesso no banco {self.database}")
#            return cursor
#            
#        except psycopg2.Error as e:
#            logger.error(f"Erro ao conectar ao banco de dados: {e}")
#            raise
#    
#    def close_connection(self, cursor):
#        """
#        Fecha a conexão com o banco de dados
#        
#        Args:
#            cursor: Cursor retornado pela função get_connection()
#        """
#        try:
#            if cursor:
#                cursor.close()
#                cursor.connection.close()
#                logger.info("Conexão fechada com sucesso")
#        except Exception as e:
#            logger.error(f"Erro ao fechar conexão: {e}")
#
#    def gerar_insight(self, row):
#        prompt = f"""
#    Analise o acidente de trabalho a seguir e gere um insight breve (máximo 2 frases), em pt-BR, considerando riscos, padrões ou anomalias:
#
#    - Data do acidente: {row['data_acidente']}
#    - Sexo: {row['sexo']}
#    - Estado do acidente: {row['estado_acidente']}
#    - Município do empregador: {row['municipio_empregador_nome']}
#    - Tipo de acidente: {row['tipo_acidente']}
#    - Agente causador: {row['agente_causador_acidente']}
#    - Natureza da lesão: {row['natureza_lesao']}
#    - Parte do corpo atingida: {row['parte_corpo_atingida']}
#    - CID diagnóstico: {row['descricao_diagnostico']}
#    - CNAE/Atividade econômica: {row['atividade_economica']}
#    - Houve óbito? {row['indica_obito_acidente']}
#
#    Escreva de forma objetiva e informativa.
#    """
#        try:
#            response = ollama.chat(
#                model='llama3', 
#                messages=[{'role': 'user', 'content': prompt}]
#            )
#            return response['message']['content'].strip()
#        except Exception as e:
#            print(f"Erro ao gerar insight: {e}")
#            return "Erro ao gerar insight"
#    
#    def load_data_from_core_to_full_dataset(self, batch_size=1000):
#        """
#        Carrega dados da core, insere em um df e retorna
#        """
#        try:
#            logger.info("Iniciando carregamento de dados da core para df_ia")
#            
#            # Primeiro, verificar se há dados na core
#            cursor = self.get_connection()
#            cursor.execute("SELECT COUNT(*) as count FROM schema_core.acidente_trabalho")
#            result = cursor.fetchone()
#            total_records = result['count']
#            
#            if total_records == 0:
#                logger.warning("Nenhum dado encontrado na tabela core principal")
#                return
#            
#            logger.info(f"Total de registros na tabela core principal: {total_records}")
#            
#            self.close_connection(cursor)
#            
#            # Processar dados em lotes
#            offset = 0
#            total_inserted = 0
#            df_ia = pd.DataFrame()
#            
#            while offset < total_records:
#                try:
#                    # Carregar lote de dados da staging
#                    query = f"""
#                    SELECT
#                        a.id,
#                        a.data_acidente,
#                        a.mes,
#                        a.indica_obito_acidente,
#                        a.natureza_lesao,
#                        a.parte_corpo_atingida,
#                        a.sexo,
#                        a.tipo_acidente,
#                        a.estado_acidente,
#                        a.estado_empregador,
#                        a.data_nascimento,
#                        a.agente_causador_acidente,
#
#                        m.municipio_ibge_descricao AS municipio_empregador_nome,
#                        cnae.cnae_descricao AS atividade_economica,
#                        cid10.cid10_descricao AS descricao_diagnostico
#
#                    FROM schema_core.acidente_trabalho a
#                    LEFT JOIN schema_core.municipio m
#                        ON a.municipio_empregador_codigo = m.municipio_ibge_codigo
#                    LEFT JOIN schema_core.cnae cnae
#                        ON a.cnae_empregador_codigo = cnae.cnae_codigo
#                    LEFT JOIN schema_core.cid10 cid10
#                        ON a.cid_10_codigo = cid10.cid10_codigo
#                    LIMIT {batch_size} OFFSET {offset}
#                    """
#                    
#                    df_batch = pd.read_sql(query, self.engine)
#                    
#                    if df_batch.empty:
#                        break
#                    
#                    # Inserir dados
#                    df_ia = pd.concat([ df_ia, df_batch], ignore_index=True)
#                    
#                    total_inserted += len(df_batch)
#                    offset += batch_size
#                    
#                    logger.info(f"Processado lote: {offset}/{total_records} registros. Inseridos: {total_inserted}")
#                    
#                except Exception as e:
#                    logger.error(f"Erro ao processar lote offset {offset}: {e}")
#                    raise
#            
#            logger.info(f"Carregamento concluído! Total de registros inseridos no df_ia: {total_inserted}")
#
#            #df_ia = df_ia.head(1)
#            df_ia['insight'] = df_ia.apply(self.gerar_insight, axis=1)
#            df_ia = df_ia[['id', 'insight']]
#
#            return df_ia
#            
#        except Exception as e:
#            logger.error(f"Erro ao carregar dados da core para o df_ia: {e}")
#            raise
#        finally:
#            self.close_connection(cursor)
#
#    def load_data_from_ai_to_core_acidente(self):
#        """
#        Carrega dados gerados pela ia e insere na tabela core
#        """
#        try:
#            logger.info("Iniciando carregamento de dados da ia para core")
#            cursor = self.get_connection()
#
#            try:
#                # Carregar dados da ia
#                df_ia = self.load_data_from_core_to_full_dataset()
#                
#                # Inserir na tabela core
#                update_query = """
#                    UPDATE schema_core.acidente_trabalho
#                    SET insight = %s
#                    WHERE id = %s;
#                """
#
#                # Cria uma lista de tuplas (insight, id)
#                update_data = list(zip(df_ia['insight'], df_ia['id']))
#                
#                execute_batch(cursor, update_query, update_data, page_size=1000)
#                
#                logger.info(f"Atualização concluída com sucesso! {len(update_data)} registros atualizados.")
#                    
#            except Exception as e:
#                logger.error(f"Erro ao atualizar insights no banco: {e}")
#                raise
#            
#            logger.info(f"Carregamento concluído! Insights atualizados.")
#            
#        except Exception as e:
#            logger.error(f"Erro ao carregar dados da staging para core: {e}")
#            raise
#        finally:
#            self.close_connection(cursor)
