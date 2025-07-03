import pandas as pd
import logging
import os
import sys

file = './info_transportes.csv'

# Pega o caminho atual que o script está localizado
script_dir = os.path.dirname(os.path.abspath(__file__))

# Cria o nome que o arquivo de log terá
log_file = os.path.join(script_dir, 'info.log')

# Configuração do logger
logging.basicConfig(
    filename=log_file,
    filemode='w',
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %I:%M:%S',
    level=logging.INFO
)

logger = logging.getLogger(__name__)

logger.info('Iniciando script de processamento')


def main():

    try:
        logger.info('Alterando formato da data para Ano-mes-dia')
        df = pd.read_csv(file, sep=';')
        df['DATA_INICIO'] = pd.to_datetime(
            df['DATA_INICIO'], format='%m-%d-%Y %H:%M', errors='coerce').dt.strftime('%Y-%m-%d')

        logger.info('Agregando informacoes das colunas')
        df_agg = df.groupby('DATA_INICIO').agg(
            QT_CORR=('DATA_INICIO', 'count'),
            QT_CORR_NEG=('CATEGORIA', lambda x: (x == 'Negocio').sum()),
            QT_CORR_PESS=('CATEGORIA', lambda x: (x == 'Pessoal').sum()),
            VL_MAX_DIST=('DISTANCIA', 'max'),
            VL_MIN_DIST=('DISTANCIA', 'min'),
            VL_AVG_DIST=('DISTANCIA', 'mean'),
            QT_CORR_REUNI=('PROPOSITO', lambda x: (x == 'Reunião').sum()),
            QT_CORR_NAO_REUNI=('PROPOSITO', lambda x: (x != 'Reunião').sum())
        )
        logger.info('Exibindo resultado agregado')
        print(df_agg)
        df_agg.to_csv('info_corridas_do_dia.csv', sep=';', index=False)
        logger.info('Resultado salvo como: info_corridas_do_dia.csv')
    except Exception as e:
        logger.error(e)
        sys.exit(1)


if __name__ == '__main__':
    main()
