#print("hbs_member_v2")
import sys
sys.path.append("/usr/lib/python2.7/site-packages")
import urllib3
import json
import base64
url = "http://hdpm2.hepsiburada.cloud:7788/"
if memberid == None:
    table_name = primarysource1
    row_key = anonymousid
    url_last = url + table_name + "/" +row_key
elif len(memberid) == 0:
    table_name = primarysource1
    row_key = anonymousid
    url_last= url + table_name + "/" +row_key
else:
    #print("else")
    table_name = primarysource1
    row_key = memberid
    url_last= url + table_name + "/" +row_key
sku = []
productcount = 0
try:
    headers_json = {'content-type':'text/json','accept':'application/json','Connection':'close'}
    http = urllib3.PoolManager()
    r = http.request(method='GET',url=url_last,headers=headers_json)
    if r.status == 200:
        a = json.loads(r.data)
        column_name_sku = primarysourcetype1 + ":sku"
        for column in a['Row'][0]['Cell']:
            if((base64.b64decode(column['column'])).decode() == column_name_sku):
                skulist = (base64.b64decode(column['$'])).decode()
        if(len(skulist)>0):
            sku = skulist.split('*')
        #print(sku)
        productcount = len(sku)
        #print(productcount)
        ScenarioId = ScenarioId1
        RecoHeader = placementDefName1
        placementId = placementId
    else:
        productcount = 0
    if(productcount<5):
        #print("productcount<5")
        if primarysource2 != '':
            url_last = url + primarysource2 + "/" + "20"
            http = urllib3.PoolManager()
            r = http.request(method='GET',url=url_last,headers=headers_json)
            if r.status == 200:
                column_name_sku = primarysourcetype2 + ":sku"
                a = json.loads(r.data)
                for column in a['Row'][0]['Cell']:
                    if((base64.b64decode(column['column'])).decode() == column_name_sku):
                        skulist = (base64.b64decode(column['$'])).decode()
                if(len(skulist)>0):
                    sku = skulist.split('*')
                #print(sku)
                ScenarioId = ScenarioId2
                RecoHeader = placementDefName2
                placementId = placementId
            else:
                sku = []
                placementId = None
        else:
            sku = []
            placementId = None
except Exception as e:
    print("Exception is:"+str(e))
    sku = []
    placementId = None
score = []