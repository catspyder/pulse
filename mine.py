import os
import pandas as pd
import numpy as np
import json

dictionary=dict()
def get_data(dir):
    
    for i in os.listdir(dir):
        # print(i)
        if i.endswith('.json'):
            df=pd.read_json(dir+'\\'+i)
            n=dir+'\\'+i
            dictionary[n]=df
        else:
            get_data(dir+'\\'+i)

            
dir=os.getcwd()+'\\data'
get_data(dir)



