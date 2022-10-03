#!/usr/bin/env python
# coding: utf-8

# In[1]:


from ftplib import FTP
import os
import numpy as np
import pandas as pd
from datetime import datetime
from io import BytesIO,StringIO
import psycopg2


# In[13]:


def FTP_download(date, date_folder):
    ftp = FTP()
    timeout = 30
    port = 21
    ftp.connect('xx.xx.xx.xx', port, timeout) # 連線FTP伺服器
    ftp.login('xx', 'xx**') # 登入
    temp_list = ftp.nlst(date_folder)       # 獲得目錄列表
    temp_list = sorted(temp_list, reverse = True)
    logfile.append(f"{datetime.now()} [success] -> 獲取資料夾 : {temp_list}")
    print(f"{datetime.now()} [success] -> 獲取資料夾 : {temp_list}")
    date_list = []

    #收集 +/-1hr的資料
    date_shift1hr=date[:-2]+str(int(date[-2:])-1).zfill(2)
    for folder in temp_list:
        if (date in os.path.basename(folder)) | (date_shift1hr in os.path.basename(folder)): 
            date_list.append(folder)
    
    logfile.append(f"{datetime.now()} [success] -> 篩選資料區間 : {date_shift1hr} <-> {date}")
    print(f"{datetime.now()} [success] -> 篩選資料區間 : {date_shift1hr} <-> {date}")

    if date_list:
        remote_folder = date_list[0] + '/MD/'# 最新日期
        remote_folder_name= date_list[0].split('/')[-1]
        logfile.append(f"{datetime.now()} [success] -> 處理資料夾內容 : {remote_folder}")
        print(f"{datetime.now()} [success] -> 處理資料夾內容 : {remote_folder}")
        remote_list = ftp.nlst(remote_folder) # 獲得目錄列表

        #先建立必要資料的完整路徑清單
        necessary_list=[]
        for doc in dataDict.keys():
            necessary_list.append(remote_folder+doc+".csv")

        for path in necessary_list:
            file_name = os.path.basename(path).split('.')[0]
            #確認是否有缺少檔案
            if path not in remote_list:
                if "WipOutPlanTime" in path: 
                    for onepath in remote_list:
                        if "WipOutPlanTime" in onepath:
                            path=onepath
                else:
                    #print(path+" : 找不到資料")
                    logfile.append(f"{datetime.now()} [err] -> {path} : 找不到資料")
                    print(f"{datetime.now()} [err] -> {path} : 找不到資料")
                    save_log()
                    continue
            
            #print(path+" -> 開始進行讀檔")
            logfile.append(f"{datetime.now()} [success] -> {path} : 開始進行讀檔")
            print(f"{datetime.now()} [success] -> {path} : 開始進行讀檔")

            try : 
                flo = BytesIO()
                ftp.retrbinary('RETR ' + path, flo.write)
                flo.seek(0)

                if file_name=="uph": #uph中header特別亂需要整理後再合併
                    flo_header = BytesIO()
                    ftp.retrbinary('RETR ' + path, flo_header.write)
                    flo_header.seek(0)
                    
                    header_0=pd.read_csv(flo_header,sep='\t', index_col=0, nrows=0).columns.tolist()[0]
                    header_1=header_0.replace('(ea/hr,set)','').replace('(K/day,set)','').replace('(K/day)','').replace(' ','_').replace('.','_')
                    header_2=pd.read_csv(StringIO(header_1),sep=',', engine='python',encoding = "ISO-8859-1")#encoding = "ISO-8859-1" utf-8
                    header_2=header_2.loc[:,~header_2.columns.str.contains('^Unnamed')]
                    header_3=header_2.columns.tolist()

                    df=pd.read_csv(flo, sep=',', engine='python',encoding = "ISO-8859-1",skipinitialspace=True)
                    df = df.iloc[:,0:len(header_3)]
                    df.columns=header_3
                                        
                else:
                    df=pd.read_csv(flo, sep=',', engine='python',encoding = "ISO-8859-1",skipinitialspace=True)

                df=df.replace('\t','', regex=True)
                df=df.replace(' ','', regex=True)
                df=df.replace('','NaN', regex=True)
                df=df.fillna('NaN')
                df=df.replace('nan','NaN', regex=True)
                df=df.replace('None','NaN', regex=True)
                df=df.loc[:,~df.columns.str.contains('^Unnamed')]
                dataDict[file_name]["header"].append(df.columns.tolist())
                dataDict[file_name]["time"]=remote_folder_name
                for index, row in df.iterrows():
                    dataDict[file_name]["data"].append(row.tolist())

                logfile.append(f"{datetime.now()} [success] -> {path} : 讀檔成功")
                print(f"{datetime.now()} [success] -> {path} : 讀檔成功")
            except Exception as e:
                logfile.append(f"{datetime.now()} [err] -> {path} : 讀檔失敗，異常訊息[{e}]")
                print(f"{datetime.now()} [err] -> {path} : 讀檔失敗，異常訊息[{e}]")
                save_log()
                raise Exception(e)
        
    else :
        logfile.append(f"{datetime.now()} [err] -> {date_folder} : 內無資料夾")
        print(f"{datetime.now()} [err] -> {date_folder} : 內無資料夾")
        save_log()

    ftp.quit()
    return True

def executeSQL(sql_list):
    try:
        db = psycopg2.connect(host="xx.xx.xx.xx",user="postgres",password="admin",dbname="APS_MD",sslmode='allow')            
        cursor = db.cursor()
        for i in range(len(sql_list)):
            cursor.execute(sql_list[i])
    except Exception as e:
        logfile.append(f"{datetime.now()} [err] -> {sql_list[i]} : executeSQL失敗")
        print(f"{datetime.now()} [err] -> {sql_list[i]} : executeSQL失敗")
        save_log()
        raise Exception(e)
    finally:
        db.commit()
        db.close()ß

def readSQL(sql_list):
    db = psycopg2.connect(host="10.3.10.203",user="postgres",password="admin20^",dbname="APS_MD",sslmode='allow')            
    data=pd.read_sql(sql_list,db)
    db.commit()
    db.close()
    return data

def save_log():
    df_logfile = pd.DataFrame(logfile, columns=["log"])
    df_logfile.to_csv(f'log/log-{year+date+time_stamp}.csv', index=False, encoding="utf-8-sig")

def insert_run_list():
    result_detail=[]
    for key, value in dataDict.items():
        result_detail.append(key+":["+value["success"]+"]")
    result_string = ";".join(result_detail)
    result="success" if "X" not in result_string else "fail"
    result_timestamp=dataDict["eim"]["time"]
    executeSQL([str(f"INSERT INTO public.data_crawler_run_list(update_time,scope,result,result_detail,timestamp) VALUES('{today}','mes','{result}','{result_string}','{result_timestamp}')")])

def insertDatatoDB():
    for key, value in dataDict.items():
        if len(dataDict[key]["data"])>0:
            try:
                insert_table=value["table"]
                insert_col=value["header"][0]
                insert_data=value["data"]
                insert_time=value["time"]
                logfile.append(f"{datetime.now()} [success] -> {insert_table} : 處理中...  ")
                print(f"{datetime.now()} [success] -> {insert_table} : 處理中...  ")

                # selectSQL=str(f"select * from public.{insert_table}")
                # print(readSQL(selectSQL))

                # (1)先刪除資料庫中timestamp相同者
                deleteSQL=str(f"delete from public.{insert_table} where timestamp='{insert_time}'")
                executeSQL([deleteSQL])
                logfile.append(f"{datetime.now()} [success] -> {insert_table} : 完成delete timestamp重複者")
                print(f"{datetime.now()} [success] -> {insert_table} : 完成delete timestamp重複者")

                # (2)資料庫中的欄位active="N"
                activeChangeSQL=str(f"update public.{insert_table} set active='N'")
                executeSQL([activeChangeSQL])
                logfile.append(f"{datetime.now()} [success] -> {insert_table} : 完成將原始資料回壓active='N'")
                print(f"{datetime.now()} [success] -> {insert_table} : 完成將原始資料回壓active='N'")

                # (3)insert 新的資料進去
                col_num=len(insert_col)
                col_content=""
                for i in range(col_num):
                    col_content+='%s,'
                
                col_content=col_content[:-1] #去除最後一個",""
                insert_sql_list=[]
                for j in range(len(insert_data)):
                    insert_sql_list.append(str(f"INSERT INTO public.{insert_table}({insert_col},timestamp,active)").replace("'","").replace('[','').replace(']','')+str(f" VALUES({insert_data[j]},'{insert_time}','Y')").replace('[','').replace(']',''))
                executeSQL(insert_sql_list)
                logfile.append(f"{datetime.now()} [success] -> {insert_table} : 完成新資料insert({insert_time})")
                print(f"{datetime.now()} [success] -> {insert_table} : 完成新資料insert({insert_time})")
                dataDict[key]["success"]="O"

            except Exception as e:
                logfile.append(f"{datetime.now()} [err] -> {insert_table} : 異常訊息[{e}]")
                print(f"{datetime.now()} [err] -> {insert_table} : 異常訊息[{e}]")
                save_log()
                raise Exception(e)
        else :
            logfile.append(f"{datetime.now()} -> dataDict沒有資料[{key}] : 跳過 DB execute")
            print(f"{datetime.now()} -> dataDict沒有資料[{key}] : 跳過 DB execute")
            save_log()


# In[14]:


# get now date & time 
today=datetime.now()
year=str(today.year)
month=str(today.month).zfill(2)
day=str(today.day).zfill(2)
hour=str(today.hour).zfill(2)
hour_before_1hr=str(today.hour-1).zfill(2)
time_stamp=hour+str(today.minute).zfill(2)+str(today.second).zfill(2)
date=month+day
dateTime=year+date+hour

# create a array to append data & header, and start to data pipline 
dataDict={
    "eim":{"header":[],"data":[],"table":"mes_eim","time":"","success":"X"},
    "gtsdat_STACKDIEOPER":{"header":[],"data":[],"table":"mes_gtsdat_stackdieoper","time":"","success":"X"},
    "ntcent":{"header":[],"data":[],"table":"mes_ntcent","time":"","success":"X"},
    "ntclot":{"header":[],"data":[],"table":"mes_ntclot","time":"","success":"X"},
    "uph":{"header":[],"data":[],"table":"mes_uph","time":"","success":"X"},
    "WipOutPlanTime":{"header":[],"data":[],"table":"mes_wipoutplantime","time":"","success":"X"},
}

# log
logfile=[]


# In[15]:


# download & process data
logfile.append(f"{datetime.now()} -> 開始進行資料讀取與處理程序...")
print(f"{datetime.now()} -> 開始進行資料讀取與處理程序...")
FTP_download(dateTime,f'MES_FTP/AutoScheduling/{year}/{date}/')
logfile.append(f"{datetime.now()} -> 完成資料讀取與處理程序。")
print(f"{datetime.now()} -> 完成資料讀取與處理程序。")

# insert processed data to DB tables
logfile.append(f"{datetime.now()} -> 開始進行DB execute...")
print(f"{datetime.now()} -> 開始進行DB execute...")
insertDatatoDB()
logfile.append(f"{datetime.now()} -> 完成進行DB execute。")
print(f"{datetime.now()} -> 完成進行DB execute。")

#finally (1)save log (2)update data_crawler_run_list
save_log()
insert_run_list()


# In[ ]:





# In[ ]:





# In[ ]:




