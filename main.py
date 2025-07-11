from db.datawarehouse import DataWarehouseSetup
from db.stage import StageSetup
from db.core import CoreSetup
from db.stage import logger
import os

def main():
    """Função principal para executar o setup do Data Warehouse"""
    try:
        # Configurações do banco
        dw_setup = DataWarehouseSetup(
            host='localhost',
            port=5432,
            database='acidente_trabalho_dw',
            user='postgres',
            password='99712003'  # ALTERE PARA SUA SENHA
        )       

        stage_setup = StageSetup(
            host='localhost',
            port=5432,
            database='acidente_trabalho_dw',
            user='postgres',
            password='99712003'  # ALTERE PARA SUA SENHA
        )   

        core_setup = CoreSetup(
            host='localhost',
            port=5432,
            database='acidente_trabalho_dw',
            user='postgres',
            password='99712003'  # ALTERE PARA SUA SENHA
        )     

        # Caminho para a pasta de dados
        data_folder = os.path.join(os.path.dirname(__file__), 'data/acidente')
        
        logger.info("=== INICIANDO SETUP DO DATA WAREHOUSE ===")
        
        # 1. Criar banco de dados
        dw_setup.create_database_if_not_exists()
        
        # 2. Criar schemas
        dw_setup.create_schemas()
        
        logger.info("=== SETUP DO DO DATA WAREHOUSE CONCLUÍDO ===")

        logger.info("=== INICIANDO SETUP DO STAGE ===") 

        # 3. Criar tabelas auxiliares no stage
        stage_setup.create_stage_table_auxiliar()

        # 4. Carregar dados CSV auxiliares para o stage
        stage_setup.load_csv_files_auxiliar()

        # 5. Criar tabela principal no stage
        stage_setup.create_stage_table_acidente()
        
        # 6. Carregar dados CSV principal para o stage
        stage_setup.load_csv_files_acidente(data_folder)
        
        logger.info("=== SETUP DO STAGE CONCLUÍDO ===")
        
        logger.info("=== INICIANDO SETUP DO CORE ===") 

        # 7. Criar tabelas auxiliares no core
        core_setup.create_core_table_auxiliar()

        # 8. Carregar dados das tabelas auxiliares do stage para o core
        core_setup.load_data_from_stage_to_core_auxiliar()

        # 9. Criar tabela principal no core 
        core_setup.create_core_table_acidente()

        # 10. Carregar dados da stage para o core
        core_setup.load_data_from_stage_to_core_acidente()
        
        logger.info("=== SETUP DO DO CORE CONCLUÍDO ===")
        
    except Exception as e:
        logger.error(f"Erro durante o setup: {e}")
        raise

if __name__ == "__main__":
    main()