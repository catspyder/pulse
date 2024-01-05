import os
import pandas as pd
import numpy as np
import json
import psycopg2
from sqlalchemy import create_engine


# Connect to the PostgreSQL database
engine = create_engine('postgresql://postgres:7263@localhost:5432/pulse')

engine = create_engine('postgresql://postgres:7263@localhost:5432/pulse')
# Perform database operations...



dictionary=dict()


def get_data(dir):
    
    for i in os.listdir(dir):
        # print(i)
        if i.endswith('.json'):
            path=os.path.join(dir,i)
            n=dir.split('\\')
            section=n[7]
            subsection=n[8]
            df=None
            with open(path,'r' ) as f:
                temp=json.load(f)
            tf=pd.json_normalize(temp['data']['transactionData'], 'paymentInstruments', ['name'])
            print
            # nm=''
            # for k in n:
            #     nm=nm+k[:2]
            # df.to_sql(nm[10:], if_exists='replace', index=False, con=engine)
            # dictionary[nm[10:]]=df
        else:
            get_data(dir+'\\'+i)

            
dir=os.getcwd()+'\\data'

get_data(dir)
# engine.close()    
print(dictionary)


