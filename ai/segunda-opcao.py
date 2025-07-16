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
#    def gerar_insight(self, estado,  df_subset):
#        try:
#            amostras = df_subset.sample(n=min(10, len(df_subset)))
#            exemplos = "\n".join([
#                f"- {row['data_acidente']}, {row['sexo']}, {row['tipo_acidente']}, Lesão: {row['natureza_lesao']}, Parte: {row['parte_corpo_atingida']}, CID: {row#['descricao_diagnostico']}, Óbito: {row['indica_obito_acidente']}, Agente: {row['agente_causador_acidente']}, Município: {row['municipio_empregador_nome']}, Setor: #{row['setor_economico']}"
#                for _, row in amostras.iterrows()
#            ])
#
#            prompt = f"""
#    Analise os acidentes de trabalho no estado {estado}.
#
#    Considere os seguintes exemplos representativos:
#    {exemplos}
#
#    Com base nos dados completos desse grupo, gere um insight breve (até 2 frases), em pt-BR, destacando padrões, riscos ou anomalias.
#    Evite repetir diretamente os dados. Foque em interpretações.
#    """
#            response = ollama.chat(
#                model='llama3',
#                messages=[{'role': 'user', 'content': prompt}]
#            )
#            return response['message']['content'].strip()
#
#        except Exception as e:
#            logger.error(f"Erro ao gerar insight para {estado}: {e}")
#            return "Erro ao gerar insight"
#
#    def create_core_table_insight(self):
#        """Cria a tabela de core para insights"""
#        try:
#            cursor = self.get_connection()
#            # SQL para criar a tabela de core
#            create_table_sql = """
#            CREATE TABLE IF NOT EXISTS schema_core.insight (
#                id SERIAL PRIMARY KEY,
#                estado VARCHAR,
#                insight TEXT
#            );
#            """
#            
#            cursor.execute(create_table_sql)
#            
#            logger.info("Tabela schema_core.insight criada com sucesso!")
#                
#        except Exception as e:
#            logger.error(f"Erro ao criar tabela de core: {e}")
#            raise
#        finally:
#            self.close_connection(cursor)
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
#                        a.estado_empregador AS estado,
#                        a.data_nascimento,
#                        a.agente_causador_acidente,
#
#                        m.municipio_ibge_descricao AS municipio_empregador_nome,
#                        cnae.cnae_descricao AS setor_economico,
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
#            try:
#                logger.info("Iniciando carregamento de dados da ia para core")
#
#                #df_ia = df_ia.head(1)
#                grouped_insights = []
#
#                for (estado), group in df_ia.groupby(['estado']):
#                    insight = self.gerar_insight(estado, group)
#                    grouped_insights.append({'estado': estado, 'insight': insight})
#                
#                df_resultado = pd.DataFrame(grouped_insights)
#
#                cursor = self.get_connection()
#
#                try:
#                    # Inserir na tabela core
#                    df_resultado.to_sql(
#                        name='insight',
#                        con=self.engine,
#                        schema='schema_core',
#                        if_exists='append',
#                        index=False,
#                        method='multi'
#                    )
#                
#                    logger.info(f"Inserção concluída com sucesso! {len(df_resultado)} registros inseridos.")
#                        
#                except Exception as e:
#                    logger.error(f"Erro ao inserir insights no banco: {e}")
#                    raise
#                
#                logger.info(f"Carregamento concluído! Insights inseridos.")
#                
#            except Exception as e:
#                logger.error(f"Erro ao carregar dados da ia para core: {e}")
#                raise
#            finally:
#                self.close_connection(cursor)
#            
#        except Exception as e:
#            logger.error(f"Erro ao carregar dados da core para o df_ia: {e}")
#            raise