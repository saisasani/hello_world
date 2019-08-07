#print('placementStrategyMapv2')
import sys
sys.path.append("/usr/lib/python2.7/site-packages")
import urllib3
reload(sys)
sys.setdefaultencoding('utf8')
import json
import base64
#url = "http://hdpm2.hepsiburada.cloud:7788/"
#table_name = "placementstrategymap"
#testgroup = "A2"
#placementid = "item_page.web-Unprocurable"
testgroup = testgroup.replace(" ","")
placementid = placementid.replace(" ","")
rowkey = testgroup + "_" + placementid
url_last= url + table_name + "/" +rowkey
headers = {'content-type':'text/json','accept':'application/json','Connection':'close'}
http = urllib3.PoolManager()
#print('Step1')
try:
    r = http.request(method='GET',url=url_last,headers=headers)
    #print('Step2')
    if r.status == 200:
        a = json.loads(r.data)
        #print('Step3')
        for column in a['Row'][0]['Cell']:
            if((base64.b64decode(column['column'])).decode() == 'placement:primarySource1'):
                primarysource1 = (base64.b64decode(column['$'])).decode()
                #print(primarysource1)
            if((base64.b64decode(column['column'])).decode() == 'placement:primarySourceType1'):
                primarysourcetype1 = (base64.b64decode(column['$'])).decode()
                #print(primarysourcetype1)
            if((base64.b64decode(column['column'])).decode() == 'placement:placementDefID1'):
                placementDefId1 = (base64.b64decode(column['$'])).decode()
                #print(placementDefId1)
            if((base64.b64decode(column['column'])).decode() == 'placement:strategy'):
                strategy = (base64.b64decode(column['$'])).decode()
                #print(strategy)
            if((base64.b64decode(column['column'])).decode() == 'placement:placementDefName1'):
                placementDefName1 = (base64.b64decode(column['$'])).decode()
                #print(placementDefName1)
            if((base64.b64decode(column['column'])).decode() == 'placement:primarySource2'):
                primarysource2 = (base64.b64decode(column['$'])).decode()
                #print(primarysource2)
            if((base64.b64decode(column['column'])).decode() == 'placement:primarySourceType2'):
                primarysourcetype2 = (base64.b64decode(column['$'])).decode()
                #print(primarysourcetype2)
            if((base64.b64decode(column['column'])).decode() == 'placement:placementDefID2'):
                placementDefId2 = (base64.b64decode(column['$'])).decode()
                #print(placementDefId2)
            if((base64.b64decode(column['column'])).decode() == 'placement:placementDefName2'):
                placementDefName2 = (base64.b64decode(column['$'])).decode()
                #print(placementDefName2)
        ScenarioId1 = testgroup + "_" + strategy + "_" + placementDefId1
        ScenarioId2 = testgroup + "_" + strategy + "_" + placementDefId2
    else:
        primarysource1 = ''
        primarysourcetype1 = ''
        placementDefName1 = ''
        ScenarioId1 = ''
        primarysource2 = ''
        primarysourcetype2 = ''
        placementDefName2 = ''
        ScenarioId2 = ''
except Exception as e:
    print("Exception is:" + str(e))
    primarysource1 = ''
    primarysourcetype1 = ''
    placementDefName1 = ''
    ScenarioId1 = ''
    primarysource2 = ''
    primarysourcetype2 = ''
    placementDefName2 = ''
    ScenarioId2 = ''