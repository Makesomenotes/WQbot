#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import requests
import json
import time
from requests.adapters import HTTPAdapter
import numpy as np
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import threading
from concurrent.futures import as_completed


# In[ ]:


s = requests.Session()
proxies = { } #personal access
def Login(): #login to site finction
    urlLogin = 'https://api.worldquantvrc.com/authentication'
    headers = { } #personal access
    req = requests.post(urlLogin, headers = headers, proxies = proxies)
    print(req.text)
    return req.headers['Set-Cookie']


# In[ ]:


Authcookie  = Login()


# In[ ]:


def Simulate(code, region, universe): #Main function. It used "code" in corporate simulator in order to test buy-sell hypothesis
    global Authcookie
    urlSimulate = 'https://api.worldquantvrc.com/simulations'
    settings = {
             'decay':0,
             'delay':1,
             'instrumentType':"EQUITY",
             'language':"FASTEXPR",
             'nanHandling':"OFF",
             'neutralization':"MARKET",
             'pasteurization':"ON",
             'region': region,
             'truncation': 0.08,
             'unitHandling':"VERIFY",
             'universe': universe,
             'visualization':1
         }
    simulationParams = {
             'code': code,
             'settings':settings,
             'type':"SIMULATE"
         }
    req = s.post(urlSimulate, data = json.dumps(simulationParams), headers = {'cookie': Authcookie, 'Content-Type': 'application/json'}, proxies = proxies)  
    if ('Location' in req.headers.keys()) == 0:
        print('No Location. Relogining')
        Authcookie = Login()
        req = s.post(urlSimulate, data = json.dumps(simulationParams), headers = {'cookie': Authcookie, 'Content-Type': 'application/json'}, proxies = proxies)  
    print(req.headers)
    return(req.headers)


# In[ ]:


def GetSimulationId(headers): #Maintaininng requests
    location = headers['Location']
    return location.replace('https://api.worldquantvrc.com:443/simulations/', '') 
def CheckSimulation(simulationId):
    url = 'https://api.worldquantvrc.com/simulations/' + simulationId
    headers = {
         'authorization': 'Basic YWRyZXNzXzIwMDlAbWFpbC5ydTpKb29tbGExMiE=', 
         'cookie':Authcookie, 
         'content-type': 'application/json'
     }
    r = requests.get(url = url, headers = headers, proxies = proxies)
    return r 


# In[ ]:


def GetAlphaInfo(code, region, universe): #Whole proccess of 1 simulation
    for i in range(0,2):
        try:
            r = Simulate(code, region, universe)
            SimulationId = GetSimulationId(r)
            break
        except:
            print('location error is happened. Pause for 30 second')
            print(str(i) + ' try' )
            time.sleep(30)        
    simulationInfo = CheckSimulation(SimulationId).content
    while b'progress' in simulationInfo:
        simulationInfo = CheckSimulation(SimulationId).content
        print(simulationInfo)
        time.sleep(5)
        if(b'"status":"ERROR"' in simulationInfo):
            print("error")
            raise KeyboardInterrupt
    return simulationInfo


# In[ ]:


def JoinTable(mainTable, other): #technical functions
    if(len(mainTable.index)  0):
        return mainTable.join(other.set_index('Date'), on = 'Date')
    else:
        return other

def CreateDataTable(Dict, code):
    DataTable = pd.DataFrame(Dict.items(), columns=['Date', code])
    return DataTable

def listmerge(lstlst):
    all=[]
    for lst in lstlst:
        all.extend(lst)
    return all
def Chunk(listik):
    chunk = [listik[x:x+9] for x in range(0, len(listik), 9)] 
    return chunk


# In[ ]:


def Huge_simulation(AlphaList, region, universe, chunk_size, simulation_size): #Whole proccess of multiple simulations
    list_of_simulation_Info= list()
    AlphaList_copy = AlphaList
    while len(AlphaList_copy)0:
        then = time.time()
        AlphaList_chunk_copy = AlphaList_copy[:chunk_size]
        AlphaList_copy = AlphaList_copy[chunk_size:]
        print('Here chunk')
        while len(AlphaList_chunk_copy)  0:
            codes_For_Simulate = AlphaList_chunk_copy[:simulation_size]
            AlphaList_chunk_copy = AlphaList_chunk_copy[simulation_size:]
            pool = ThreadPoolExecutor(simulation_size)
            futures = [pool.submit(GetAlphaInfo, code, region, universe) for code in codes_For_Simulate]
            results = [r.result() for r in as_completed(futures)]
            list_of_simulation_Info.append(results)
            print('Here is current length of the list of Sim Info ' + str(len(list_of_simulation_Info)))
            if (len(AlphaList_chunk_copy) == 0) and (len(AlphaList_copy) != 0):
                print('Here sleep for the rest of a 10 minutes')
                print("Threadpool done in %s" % (time.time()-then))
                print('Here is current length of the list of Sim Info ' + str(len(list_of_simulation_Info)))
                time.sleep(60*10)           
    merged_Alpha_Info = listmerge(list_of_simulation_Info)
    print('Here is length of merged Alpha Info ' + str(len(merged_Alpha_Info))) 
    list_alpha_Ids = list()
    for i in range(len(merged_Alpha_Info)):
         if (b'alpha' in merged_Alpha_Info[i]) != 0:
            print(i)
            list_alpha_Ids.append(json.loads(merged_Alpha_Info[i])['alpha'])
        else:
            print('Bad'+str(i))
            d = GetAlphaInfo(AlphaList[i], region, universe)
            list_alpha_Ids.append(json.loads(d)['alpha'])
            ++i  
    return(list_alpha_Ids) #return is a list of ids, which are collected on platform


# In[ ]:


def GetPnl(alphaId): #getting specific results from 1 simulation
    url = 'https://api.worldquantvrc.com/alphas/'+ alphaId + '/recordsets/daily-pnl'
    urlRecordsets = 'https://api.worldquantvrc.com/alphas/'+ alphaId + '/recordsets'
    headers = { 
       'cookie':Authcookie, 
       'content-type': 'application/json'
        }
    r = requests.get(url = url, headers = headers, proxies = proxies)
    while 'retry-after' in r.headers:
            time.sleep(float(r.headers['retry-after']))
            r = requests.get(url = url, headers = headers, proxies = proxies)  
    serverpnl = np.array(r.json()['records']);
    pnl = {date : _pnl for date, _pnl in zip(serverpnl[:, 0], serverpnl[:, 1].astype(float))}
    return pnl
    
def GetTurn(alphaId):
    urlTurn = 'https://api.worldquantvrc.com/alphas/' +  alphaId + '/recordsets/turnover'
    headers = { 
        'cookie':Authcookie, 
        'content-type': 'application/json'
    }
    r = requests.get(url = urlTurn, headers = headers, proxies = proxies)
    while 'retry-after' in r.headers:
            time.sleep(float(r.headers['retry-after']))
            r = requests.get(url = urlTurn, headers = headers, proxies = proxies)
    serverturnover = np.array(r.json()['records']);    
    turnover = {date : _turnover for date, _turnover in zip(serverturnover[:, 0], serverturnover[:, 1].astype(float))}  
    return turnover

def GetCov(alphaId):
    urlCov = 'https://api.worldquantvrc.com/alphas/' +  alphaId + '/recordsets/coverage'
    headers = { 
        'cookie':Authcookie, 
        'content-type': 'application/json'
    }
    r = requests.get(url = urlCov, headers = headers, proxies = proxies)
    while 'retry-after' in r.headers:
            time.sleep(float(r.headers['retry-after']))
            r = requests.get(url = urlCov, headers = headers, proxies = proxies)
    servercov = np.array(r.json()['records']);    
    cov = {date : _cov for date, _cov in zip(servercov[:, 0], servercov[:, 1].astype(float))}  
    return cov


# In[ ]:


def GetAl(AlphaID): #getting specific results from multiple simulations
    alphasIds = AlphaID
    pnl = [GetPnl(Id) for Id in alphasIds]
    turn =[GetTurn(Id) for Id in alphasIds]
    cov = [GetCov(Id) for Id in alphasIds]
    blankPnl = pd.DataFrame()
    blankCov = pd.DataFrame()
    blankTurn = pd.DataFrame()
    for i in range(len(AlphaList)):
        print(i)
        SinglePnlTable = CreateDataTable(pnl[i], AlphaList[i])
        SingleCovTable = CreateDataTable(cov[i], AlphaList[i])
        SingleTurnTable = CreateDataTable(turn[i], AlphaList[i])
        blankPnl = JoinTable(blankPnl, SinglePnlTable)
        blankCov = JoinTable(blankCov, SingleCovTable)
        blankTurn = JoinTable(blankTurn, SingleTurnTable)
    return (blankPnl, blankCov, blankTurn)
def GetAlinExcell(AlphaID, fileName):
    AllTable = GetAl(AlphaID)
    PnLTable = AllTable[0]
    CovTable = AllTable[1]
    TurnTable = AllTable[2]
    PnLTable.set_index('Date', inplace=True)
    CovTable.set_index('Date', inplace=True)
    TurnTable.set_index('Date', inplace=True)
    AverageCoverage = pd.DataFrame(CovTable.mean(axis=0), columns=['Average coverage'])
    AverageTurn = pd.DataFrame(TurnTable.mean(axis=0), columns=['Average turnover'])
    MaxTurn = pd.DataFrame(TurnTable.max(axis=0), columns=['Max turnover'])
    MinCoverage = pd.DataFrame(CovTable.min(axis=0), columns=['Min coverage'])
    FirstDayCovList = list()
    for column in CovTable.columns:
        all_ind = (CovTable.loc[CovTable[column]==300].index)
        if len(all_ind) > 0:
             first_day = all_ind[0]
        else:
             first_day = 'NaN'
    FirstDayCovList.append(first_day)
    FirstDayCovList = [(CovTable.loc[CovTable[column]  300].index)[0] for column in CovTable.columns] 
    FirstDayCov = pd.DataFrame({'Data': AlphaList, 'Coverage 10% date': FirstDayCovList})
    FirstDayCov.set_index('Data', inplace=True)
    ShortDataStats = pd.concat([AverageTurn, MaxTurn, AverageCoverage, MinCoverage, FirstDayCov], axis=1, sort=False)
    DataCorrelation = PnLTable.corr()
    MarkDataCorrelation = DataCorrelation.style.applymap(highlight_max)
    writer = pd.ExcelWriter(fileName+'.xlsx', engine='xlsxwriter')
    PnLTable.to_excel(writer, sheet_name='DailyPnl')
    CovTable.to_excel(writer, sheet_name='DailyCov')
    TurnTable.to_excel(writer, sheet_name='DailyTurn')
    DataCorrelation.to_excel(writer, sheet_name='Correlation')
    ShortDataStats.to_excel(writer, sheet_name='Summary')
    writer.save() 


# In[ ]:




