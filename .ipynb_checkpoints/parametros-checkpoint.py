import pandas as pd
from sqlalchemy import table, column, select, types, Float
from sqlalchemy.dialects import oracle


class ParamApdw:
    def __init__(self):
        self.arquivo_origem= "/tmp/daskdata/source/APDW_CARGA.txt"
        self.url_db= 'oracle+cx_oracle://c##pocdask:pocdask@vm-oracle-xe2.c.poc-dask-porto.internal:1521/XE'
        self.raw_schema_base={
             'COD':'Int64',
             'COD_RA': 'Int64',
             'COD_MO': 'Int64',
             '00AAAJ': 'str',
             'DAT_ULT_AT': 'str',
             'QTD_DOC_SEG_N': 'Int64',
             'QTD_DOC_SEG_R': 'Int64',
             'VLR_DOC_SEG_EMIT': 'Float64',
             'VLR_PREMIO_COBRA': 'Float64',
             'VLR_SINISTRO_AVI': 'Float64',
             'VLR_SINISTRO_PAG': 'Float64'
        }
        self.schema_base={
            'COD':'Int64',
            'COD_RA': 'Int64',
            'COD_MO': 'Int64',
            '00AAAJ': 'str',
            'DAT_ULT_AT': 'datetime64[ns]',
            'QTD_DOC_SEG_N': 'Int64',
            'QTD_DOC_SEG_R': 'Int64',
            'VLR_DOC_SEG_EMIT': 'Float64',
            'VLR_PREMIO_COBRA': 'Float64',
            'VLR_SINISTRO_AVI': 'Float64',
            'VLR_SINISTRO_PAG': 'Float64'
            }

        self.oracle_types={
            'Int64':oracle.NUMBER,
            'str':types.VARCHAR(6),
            'datetime64[ns]':oracle.DATE(),
            'Float64':oracle.FLOAT(binary_precision=53),
            
        }

        
        self.meta_schema=pd.DataFrame({ 
            #'cod': pd.Series([],name='cod', dtype= 'Int64'),
            'cod_ra': pd.Series([],name='cod_ra', dtype= 'Int64'),
            'cod_mo': pd.Series([],name='cod_mo', dtype= 'Int64'),
            '00AAAJ': pd.Series([],name='00AAAJ', dtype= 'str'),
            'dat_ult_at': pd.Series([],name='dat_ult_at', dtype= 'datetime64[ns]'),
            'qtd_doc_seg_n': pd.Series([],name='qtd_doc_seg_n', dtype= 'Int64'),
            'qtd_doc_seg_r': pd.Series([],name='qtd_doc_seg_r', dtype= 'Int64'),
            'vlr_doc_seg_emit': pd.Series([],name='vlr_doc_seg_emit', dtype= 'Float64'),
            'vlr_premio_cobra': pd.Series([],name='vlr_premio_cobra', dtype= 'Float64'),
            'vlr_sinistro_avi': pd.Series([],name='vlr_sinistro_avi', dtype= 'Float64'),
            'vlr_sinistro_pag': pd.Series([],name='vlr_sinistro_pag', dtype= 'Float64')
        })


        
    def pandas_oracle(self,chaves):
        pa_or={
            k.lower(): self.oracle_types[v] for (k,v) in self.schema_base.items() if k.lower() in chaves
        }
        return pa_or
    def raw_pandas_oracle(self,chaves):
        pa_or={
            k: self.oracle_types[v] for (k,v) in self.schema_base.items() if k in chaves
        }
        return pa_or

