from db.staging import *
import os

def main():
    """Função principal para executar o setup do Data Warehouse"""
    try:
        # Configurações do banco (ajuste conforme necessário)
        dw_setup = DataWarehouseSetup(
            host='localhost',
            port=5432,
            database='acidente_trabalho_dw',
            user='postgres',
            password='99712003'  # ALTERE PARA SUA SENHA
        )       
        # Caminho para a pasta de dados
        data_folder = os.path.join(os.path.dirname(__file__), 'data')
        
        logger.info("=== INICIANDO SETUP DO DATA WAREHOUSE ===")
        
        # 1. Criar banco de dados
        dw_setup.create_database_if_not_exists()
        
        # 2. Criar schemas
        dw_setup.create_schemas()
        
        # 3. Criar tabela de staging
        dw_setup.create_staging_table()
        
        # 4. Carregar dados CSV
        dw_setup.load_csv_files(data_folder)
        
        logger.info("=== SETUP CONCLUÍDO COM SUCESSO ===")
        
    except Exception as e:
        logger.error(f"Erro durante o setup: {e}")
        raise

if __name__ == "__main__":
    main()