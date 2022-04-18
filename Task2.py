import argparse
from subprocess import Popen , PIPE
import os
from os import listdir
import json 
import pandas as pd
import numpy as np
import re
import httpagentparser
import time

start_time = time.time()

parser = argparse.ArgumentParser()

parser.add_argument("directoryPath",help = "enter the directory path")
parser.add_argument("-u",action="store_true",default = False,dest="unix",help="Time in unix format")
args = parser.parse_args()


files = [item for item in listdir(args.directoryPath) if (".json" in item)]

checksums = {}
duplicates = [] 

for filename in files:
        with Popen(['md5sum',filename],stdout = PIPE) as proc:
            checksum = proc.stdout.read().split()[0]
            if(checksum in checksums):
                duplicates.append(filename)
                print("Found duplicate of file {}".format(filename))
            else:
                checksums[checksum] = filename
   
for duplicatedfile in duplicates:
        os.remove(duplicatedfile)
    
for file in files:
        if file not in duplicates:
            data = [json.loads(lines) 
for lines in open(file) 
        if '_heartbeat_' not in json.loads(lines)]
        
            df = pd.json_normalize(data)
            df = df.dropna()
            
            #Web browser
            def BrowserName(useragent):
                try:
                   browser = httpagentparser.detect(useragent)['browser']['name']
                except:
                   browser = "Browser not defined"
                return browser
            
            df['Web_browser'] = df['a'].apply(BrowserName)
        
            #operating system
            def operating_system(useragent):
                try:
                    OS = httpagentparser.detect(useragent)['os']['name']
                except:
                    OS = "OS not defined"
                return OS
                
            df['operating_system'] = df['a'].apply(operating_system)
            
            #cleaning operating system from non-characters
            #df['operating_system'] = df['operating_system'].str.replace('[^a-zA-Z]', ' ', regex=True)
            
            #shorten_url 
            def shorten_url(url):
                if url is not np.nan:
                  ls = re.findall("\//(.*?)\/", url)
                if len(ls):
                    url = ls[0]
                return url
                
            #from_url    
            df['from_url'] = df['r'].apply(shorten_url)
  
            #to_url
            df['to_url'] = df['u'].apply(shorten_url)

            #city
            df['city'] = df['cy']
            
            #longitude and latitude 
            df['longitude']=df['ll'].str[0]
            df['latitude']=df['ll'].str[1]
                     
            #time_Zone
            df['time_zone'] = df['tz']
             
            
            if(args.unix):
                df['time_in'] = df['t']
                df['time_out'] = df['hc']
            else:
                df['time_in'] = pd.to_datetime(df['t'],unit='s')
                df['time_out'] =pd.to_datetime(df['hc'], unit='s')
            
          
            df = df[['Web_browser','operating_system','from_url','to_url','city','longitude','latitude','time_zone','time_in','time_out']]
            output_path = os.getcwd()
            print("{} rows available from file {} ".format(df.shape[0],file))    
            TransformedFiles = str(file)
            df.to_csv('.//target/'+TransformedFiles+'.csv')
            print("File output path:", output_path)
            
            end_time = time.time()
            execution_time = end_time - start_time
            print("Total execution time: ",execution_time)

               

