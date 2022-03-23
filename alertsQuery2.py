"""
# Bucle para recuperar todas las alertas de todas las tablas del IC vía el API de Hasura.

Versión para sacar sólo las que se han modificado desde la última vez

"""

import requests
import json
import sys
import os
import shutil
import subprocess
import time
import warnings
import pandas as pd

ct = 0

# Función para imprimir los json de resultados en modo "pretty"
def printResponse(resp):
  print(80*'-')
  print(resp)
  parsed = json.loads(resp.text)
#  print(json.dumps(parsed, indent=2, sort_keys=False))
  return parsed

from requests.packages.urllib3.exceptions import InsecureRequestWarning
warnings.simplefilter('ignore',InsecureRequestWarning)

print('Program initiated')

diaHora = time.strftime("%Y%m%d%H%M%S")

# Recupera el último timestamp tratado en la ejecución anterior.

lastTimearch = open('lastTimestamp.txt', 'r')
lastTime = lastTimeAnt = lastTimearch.read()
lastTimearch.close()
if lastTimeAnt == '': lastTimeAnt = '2022-01-01T01:01:01.558387Z[UTC]'

# Escribe la cabecera del csv de salida
sep = ';'
archivo = open('Alertas_'+diaHora+'.csv', 'w')
archivo.write('"Legal Entity Code"'+sep+'"Alert ID"'+sep+'"Party ID"'+sep+'"Creation date"'+sep+'"Origin System"'+sep+'"Status"'+sep+'"Closing date"'+sep+'"Typology"'+sep+'"Reasoning"'+sep+'"Conclusion"'+sep+'"Alert link"'+sep+'"Description Lvl1N.A."'+sep+'"Description Lvl2N.A."'+sep+'"Description Lvl3N.A."'+sep+'"Reference Date"\n')

url = "https://graphql-gsc-thetaray-infra-dev.apps.ocpdes01.gsc.dev.weu1.azure.paas.cloudcenter.corp/v1/graphql"

# Paso 1. Recorrer la tabla gsc_thetaray_apps_dev.monitoring_table para obtener las tablas de alertas manejadas en el IC

tabla = 'gsc_thetaray_apps_dev_monitoring_table'

# Nota: en el CDD actual de desarrollo esta tabla está vacía y no sale en Hasura.

# Suponemos que obtenemos las dos tablas que hay actualmente en el entorno:

tablas = ['gsc_thetaray_apps_dev_tr_alert_table_1646061612204', 'gsc_thetaray_apps_dev_tr_alert_table_1646152408307']

for tabla in tablas:
  
  payload="{\"query\":\"query MyQuery($_gt: String = \\\""+lastTimeAnt+"\\\") {\\r\\n  "+tabla+"(where: {updatedate: {_gt: $_gt}}) {\\r\\n    id\\r\\n    workflowversion\\r\\n    workflowidentifier\\r\\n    updatedate\\r\\n    triggers\\r\\n    suppressioncount\\r\\n    stateid\\r\\n    sla\\r\\n    severity\\r\\n    resolutioncode\\r\\n    recommendedresolutioncode\\r\\n    queueidentifier\\r\\n    processinstanceid\\r\\n    isclosed\\r\\n    isconsolidated\\r\\n    note\\r\\n    processdefinitionid\\r\\n    createdate\\r\\n    consolidationcount\\r\\n    attachments\\r\\n    assignee\\r\\n    alertmapperversion\\r\\n    alertmapperidentifier\\r\\n  }\\r\\n}\",\"variables\":{}}"
  
  headers = {
    'x-hasura-admin-secret': 'P@ssw0rd',
    'Content-Type': 'application/json'
  }

  response = requests.request("POST", url, headers=headers, data=payload, verify=False)

  parseado = printResponse(response)

  for i in range(len(parseado['data'][tabla])):
    entidad = 'PEND'
    id = parseado['data'][tabla][i]['id']
    party= parseado['data'][tabla][i]['triggers'][0]['alertFields']['account_id']
    fecCrea = parseado['data'][tabla][i]['triggers'][0]['createDate']
    fecCrea = fecCrea[:10]
    uno = parseado['data'][tabla][i]['triggers'][0]['risk']['id']
    dos = parseado['data'][tabla][i]['triggers'][0]['risk']['name']
    tes = parseado['data'][tabla][i]['triggers'][0]['risk']['category']
    cua = parseado['data'][tabla][i]['triggers'][0]['risk']['description']
    tipo = uno+' '+dos+' '+tes+' '+cua
    origin = "TM - thetaray"
    base_url = 'https://gsc-thetaray-apps-dev.apps.ocpdes01.gsc.dev.weu1.azure.paas.cloudcenter.corp/#/investigation-center/alert-details/'
    res1_url = '?mapperIdentifier='
    res2_url = '&tabId=RISK_DETAILS'
    mapperid = parseado['data'][tabla][i]['alertmapperidentifier']
    link = base_url+id+res1_url+mapperid+res2_url

    fecRef = parseado['data'][tabla][i]['updatedate']
    if fecRef > lastTime:lastTime=fecRef
    fecRefForm = fecRef[:19]
    status = parseado['data'][tabla][i]['stateid']
    if status in['state_closed','state_escalated_11','state_escalated_12','state_escalated_13']:
        fecClose = fecRef[:10]
        reason = parseado['data'][tabla][i]['resolutioncode']
        concl  = parseado['data'][tabla][i]['note'][-1]['body']
        concl  = concl.replace('\n', ' ')
    else: 
        fecClose = ''
        reason = ''
        concl = ''

    archivo.write(entidad+sep+id+sep+party+sep+fecCrea+sep+origin+sep+status+sep+fecClose+sep+tipo+sep+reason+sep+concl+sep+link+sep+sep+sep+sep+fecRefForm+sep+'\n')
    ct = ct + 1
    if ct%100 == 0: print('Writting record # '+str(ct))

"""
    print('Legal Entity Code: '+entidad)
    print('Alert ID: '+id)
    print('Party ID: '+party)
    print('Creation date: '+fecCrea)
    print('Origin System: '+origin)
    print('Status: '+status)
    print('Closing date: '+fecClose)
    print('Typology: '+tipo)
    print('Reasoning: '+reason)
    print('Conclusion: '+concl)
    print('Alert link: '+link)
    print('Description Lvl1: N.A.')
    print('Description Lvl2: N.A.')
    print('Description Lvl3: N.A.')
    print('Reference Date: '+fecRef)
    
    print(100*'=')
"""
# Guarda la fecha más reciente de actualización

lastTimearch = open('lastTimestamp.txt', 'w')
lastTimearch.write(lastTime)
lastTimearch.close()

print('Total records written from '+lastTimeAnt+' thru '+lastTime+': '+str(ct))