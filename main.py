from db.datawarehouse import DataWarehouseSetup
from db.stage import StageSetup
from db.core import CoreSetup
from db.mart import MartSetup
from ai.insight import IASetup
from db.stage import logger
from dotenv import load_dotenv
import os

load_dotenv()

def main():
    """Função principal para executar o setup do Data Warehouse"""
    try:
        # Configurações do banco
        dw_setup = DataWarehouseSetup(
            host=os.getenv('DB_HOST'),
            port=int(os.getenv('DB_PORT')), 
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )       

        stage_setup = StageSetup(
            host=os.getenv('DB_HOST'),
            port=int(os.getenv('DB_PORT')), 
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )   

        core_setup = CoreSetup(
            host=os.getenv('DB_HOST'),
            port=int(os.getenv('DB_PORT')), 
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )    

        mart_setup = MartSetup(
            host=os.getenv('DB_HOST'),
            port=int(os.getenv('DB_PORT')), 
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )

        ia_setup = IASetup(
            host=os.getenv('DB_HOST'),
            port=int(os.getenv('DB_PORT')), 
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )    

        # Caminho para a pasta de dados
        data_folder = os.path.join(os.path.dirname(__file__), 'data/acidente')
        
        logger.info("=== INICIANDO SETUP DO DATA WAREHOUSE ===")

#        # 0. Deletar banco de dados para atualizações
#        dw_setup.delete_database()
#        
#        # 1. Criar banco de dados
#        dw_setup.create_database_if_not_exists()
#        
#        # 2. Criar schemas
#        dw_setup.create_schemas()
#        
#        logger.info("=== SETUP DO DO DATA WAREHOUSE CONCLUÍDO ===")
#
#        logger.info("=== INICIANDO SETUP DO STAGE ===") 
#
#        # 3. Criar tabelas auxiliares no stage
#        stage_setup.create_stage_table_auxiliar()
#
#        # 4. Carregar dados CSV auxiliares para o stage
#        stage_setup.load_csv_files_auxiliar()
#
#        # 5. Criar tabela principal no stage
#        stage_setup.create_stage_table_acidente()
#        
#        # 6. Carregar dados CSV principal para o stage
#        stage_setup.load_csv_files_acidente(data_folder)
#        
#        logger.info("=== SETUP DO STAGE CONCLUÍDO ===")
#        
#        logger.info("=== INICIANDO SETUP DO CORE ===") 
#
#        # 7. Criar tabelas auxiliares no core
#        core_setup.create_core_table_auxiliar()
#
#        # 8. Carregar dados das tabelas auxiliares do stage para o core
#        core_setup.load_data_from_stage_to_core_auxiliar()
#
#        # 9. Criar tabela principal no core 
#        core_setup.create_core_table_acidente()
#
#        # 10. Carregar dados da stage para o core
#        core_setup.load_data_from_stage_to_core_acidente()
#
#        # 11. Criar tabela insight no core
#        ia_setup.create_core_table_insight()
#
#        # 12. Carregar insights da ia para o core
#        ia_setup.load_data_from_core_to_full_dataset()

        # 13. Carregar insights pré-gerados (comentar passo 12 entao)
        ia_setup.load_csv_file_insight()
        
        logger.info("=== SETUP DO DO CORE CONCLUÍDO ===")

        logger.info("=== INICIANDO SETUP DO MART ===") 

        # 14. Criar views de mart
#        mart_setup.create_mart_views()

        logger.info("=== SETUP DO DO MART CONCLUÍDO ===")
        
    except Exception as e:
        logger.error(f"Erro durante o setup: {e}")
        raise

if __name__ == "__main__":
    main()