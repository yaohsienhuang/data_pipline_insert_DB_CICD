import pandas as pd
from datetime import datetime
import psycopg2

def readCSVtoCreateTable(data,table_nmae):
    # header
    df_header=pd.read_csv(data,sep=',', engine='python', index_col=0, nrows=0)
    df_header=df_header.loc[:,~df_header.columns.str.contains('^Unnamed')]
    header=df_header.columns.tolist().replace(' ','_').replace('.','_')

    # define the column data-type
    datatype={
        "int":"double precision",
        "float":"double precision",
        "bool":"text",
        "str":"text",
        "chr":"text",
        "list":"text",
        "dict":"text",
        "tuple":"text",
        "object":"text"
    }

    # read csv contents and replace header
    df=pd.read_csv(data, sep=',', engine='python',encoding = "ISO-8859-1",skipinitialspace=True)
    df = df.iloc[:,0:len(header)]
    df.columns=header
    df=df.replace('\t','', regex=True)
    df=df.replace(' ','', regex=True)
    df=df.fillna('NaN')
    df=df.replace('nan','NaN', regex=True)
    df=df.replace('None','NaN', regex=True)

    #check types
    header_type={}
    for hd in header:
        df_hd=df[hd]
        df_hd=pd.to_numeric(df_hd, errors='ignore')
        header_type[hd]=datatype[str(df_hd.dtype).replace('8','').replace('16','').replace('32','').replace('64','').replace('128','')]
    
    #dict to sql_list
    sql_args_text=' text COLLATE pg_catalog."default", '
    sql_args_precision=' double precision, '
    sql_args=""
    for col, type in header_type.items():
        if type=="text":
            sql_args+=(col+sql_args_text)
        else :
            sql_args+=(col+sql_args_precision)

    sql_list = f"""
        CREATE TABLE IF NOT EXISTS public.{table_nmae}
        (
            {sql_args}"timestamp" text COLLATE pg_catalog."default", active text COLLATE pg_catalog."default"
        )

        TABLESPACE pg_default;

        ALTER TABLE IF EXISTS public.{table_nmae}
            OWNER to postgres;
    """
    createTable([sql_list])

def createTable(sql_list):
    try:
        db = psycopg2.connect(host="10.3.10.203",user="postgres",password="admin20^",dbname="APS_MD",sslmode='allow')            
        cursor = db.cursor()
        for i in range(len(sql_list)):
            cursor.execute(sql_list[i])
            print(f"{datetime.now()} [success] -> {sql_list[i]} : executeSQL完成")
    except Exception as e:
        print(f"{datetime.now()} [err] -> {sql_list[i]} : executeSQL失敗")
        raise Exception(e)
    finally:
        db.commit()
        db.close()


