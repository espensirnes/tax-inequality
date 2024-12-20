#!/usr/bin/python
# -*- coding: UTF-8 -*-

#A module that fetch accouting data from the net

import sys
sys.path.append('../../')
import urllib.request
import Functions as fu
import re
import datetime as dt
import csv
#import google
import time
import numpy as np
import datetime
import DB
from bs4 import BeautifulSoup
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager


db='research'
maxyear=None
minyear=2015

def GetData():
	if True:
		driver=proff_login()
	else:
		driver=None
	tbl='account_400'
	conn,crsr=DB.connect(db)
	DB.create_table(tbl,conn,crsr)
	sqlstr=DB.get_sql_insert_str(tbl,columns)
	oidlist=DB.fetch(oidlist_sql, crsr)
	k=0
	for cid,name,orgid in oidlist:
		if (k>=0):# and (orgid in [811176702]):
			print ( 'Appending %s   %s ' %(cid,name),)
			AppendData(orgid,cid,name,conn,crsr,sqlstr,driver)
			print ( ' done')
			sleep()
		k+=1
		
	DB.create_index(conn,crsr,tbl,db)
	
def not_parsed_year(t,compID,crsr):
	notparsed={0:dict(),1:dict()} #0: no corp, 1: corp
	for corp, year ,description,value,DescrID, acctype,currency  in t:
		if not year in notparsed[corp]:
			notparsed[corp][year]=not_parsed(compID, year, crsr,corp)
	return notparsed
			
def not_parsed(cid,year,crsr,corp):
	crsr.execute(f"""select distinct [CompanyID] from
   [OSEData].[dbo].[account_brnsnd]
   where [CompanyID]={cid}  and  [Year]={year} and [IsCorporateAccount]={int(corp)}""")
	r=crsr.fetchall()
	return len(r)==0
	

def sleep():
	now=datetime.datetime.now()
	wakeup=datetime.datetime(now.year,now.month,now.day,8,3,27)+datetime.timedelta(days=1)
	gohome=datetime.datetime(now.year,now.month,now.day,17,58,36)				
	
	while now>gohome and now<wakeup:
		sleeptime=3000
		time.sleep(0+sleeptime)
		now=datetime.datetime.now()

	sleeptime=60
	time.sleep(0+sleeptime*np.random.random())

	
	
	
def AppendData(ID,compID,Name,conn,crsr,sqlstr,driver):
	if not driver is None:
		source,t,webName=proff_GetDataFromID(ID,Name,driver)
	else:
		source,t,webName=GetDataFromID(ID,Name)
	tbl=[]
	if len(t)==0:
		#d=[True,ID,compID,Name,webName,dt.datetime.now(),dt.datetime.now().year,'NoData',None,None, None, None]
		#crsr.execute(sqlstr,tuple(d))
		#conn.commit()	
		if webName=='no data':
			return
		return
	
	notparsed=not_parsed_year(t, compID, crsr)
	for corp, year ,description,value,DescrID, acctype,currency  in t:#DescrID identifies row and column as a two digit float (don't ask why)
		if int(year)>0 and notparsed[corp][year]:
			if not value is None:
				value=value.replace('−','-')
			d=[corp,ID,compID,Name,webName,dt.datetime.now()]#corporate accounting,ID (organization number), CompanyID,Name
			d.extend([year,acctype ,description,value,DescrID,source,currency])#
			crsr.execute(sqlstr,tuple(d))
			try:
				conn.commit()
			except:
				pass

def proff_login():
	url="https://www.forvalt.no/"
	path = ChromeDriverManager().install()
	driver = webdriver.Chrome(service=webdriver.ChromeService(path))
	driver.get(url)
	login=driver.find_elements_by_id('loginBox')
	inputs=login[0].find_elements_by_tag_name('input')
	inputs[0].send_keys('')
	inputs[1].send_keys('')
	return driver	
	
def proff_GetDataFromID(ID,Name,driver):
	url=f"https://www.forvalt.no/ForetaksIndex/Firma/Regnskapstall/{ID}"
	driver.get(url)
	t,webname,hascorp=proff_getperiods(driver,False,Name,ID)
	if hascorp and len(t)>0:
		driver.get(url+'?AccountingType=CompanyGroup')
		t_corp,webname,hascorp=proff_getperiods(driver,True,Name,ID)
		t=np.concatenate([t,t_corp],0)
	return 'proff',t,webname
	
	
def proff_getperiods(driver,corp,Name,ID):
	html=driver.page_source
	bs=BeautifulSoup(html,"lxml")
	try:
		webname=proff_getwebname(bs)
	except IndexError:
		print(f'IndexError: cant get name of {Name}({ID}). Possibly no accounting data yet')
		return [],'no data',False
	if 'Ingen data' in driver.find_elements_by_class_name('mb-3')[3].text:
		print('No data for %s' %(webname,))
		return [],'no data',False
	for i in range(10):
		tbl=proff_iterate_yearsel(corp, driver,html,webname)
		if len(tbl)==0:
			print('Error fetching table for %s, attempt %s' %(webname,i))
		else:
			break
	time.sleep(0+10*np.random.random())
	hascorp='<a class="nav-link" title="Konsernregnskap"' in html
	return tbl, webname,hascorp
		
		
def proff_iterate_yearsel(corp,driver,html,webname):
	idstr="mainContentPlaceHolder_mainContentPlaceHolder_mainContentPlaceHolder_AccountingNumberTableUc_accountingYearRangeDropDown"
	t=[]
	curr=None
	if idstr in html:
		dropdown=driver.find_element_by_id(idstr)
		itms=dropdown.find_elements_by_tag_name('option')
		n=len(itms)
	
		for i in range(n):#iterating over year selection
			if i>0:
				dropdown=driver.find_element_by_id(idstr)
				itms=dropdown.find_elements_by_tag_name('option')	
			elemtxt=itms[i].text
			#print("%s of %s, %s" %(i,n,elemtxt))
			itms[i].click()		
			tmp,curr=proff_gettables(corp,driver,elemtxt,webname,curr)

			if len(tmp)==0: return []
			t.append(tmp)
			time.sleep(5+10*np.random.random())
		tbl=np.concatenate(t,0)
	else:
		tbl,curr=proff_gettables(corp,driver,'no dropdown',webname,curr)
	return tbl
	
	
def proff_gettables(corp,driver,text,webname,curr):
	html=driver.page_source
	bs=BeautifulSoup(html,"lxml")
	time.sleep(2)#remedy to a problem that the page does not update
	html=driver.page_source
	bs=BeautifulSoup(html,"lxml")
	tables=bs.find_all('table')
	if len(tables)==0:
		print('No table for %s, %s' %(webname,text))
		return []
	t=[]
	k=0
	for i in range(3):
		ti=proff_gettable(tables[i])
		if not ti is None and k<2:
			t.append(ti)
			k+=1
		
	ltbl,curr=proff_longformat(t,corp,curr)
	return ltbl,curr


def proff_longformat(tbls,corp,curr):
	getcurr=False
	if curr is None:
		curr=dict()
		getcurr=True
	if len(tbls)==0:
		return []
	outtbl=[]
	for k in range(len(tbls)):
		if not tbls[k] is None:
			t=tbls[k]
			years=t[0]
			for i in range(1,len(years)):
				if getcurr:
					curr[years[i]]='NOK'
				for j in range(1,len(t)):
					descr=t[j,0]
					atype= years[0]
					if descr=='Valutakode':
						curr[years[i]]=t[j,i]
					elif not descr in ['Konsernregnskap','Startdato','Avslutningsdato']:
						v=t[j,i]
						v=v.replace(',','')
						if v=='':
							v=None
						outtbl.append([corp, years[i] ,descr,v,k+0.01*j,atype,curr[years[i]]])
	outtbl=np.array(outtbl)
	return outtbl,curr

def proff_getwebname(bs):
	webname=bs.find_all('h5', {'class':"modal-title"})
	webname=str(webname[3].contents[0]).strip().title()
	if webname[-3:]==' As':
		webname=webname[:-3]+' AS'
	if webname[-4:]==' Asa':
		webname=webname[:-4]+' ASA'
	return webname
	
	
def proff_gettable(tbl):
	t=[]
	k=0
	ncols=0
	for i in tbl.find_all('tr'):
		r=[]
		th=i.find_all('th')[0]
		if th.text=='År':
			return
		abbr=th.find_all('abbr')
		if len(abbr)>0:
			h=str(abbr[0].contents[0])
		else:
			h=str(th.contents[0])
		r.append(h)
		for j in i.find_all('td'):
			c=str(j.contents[0])
			if str(type(j.contents[0]))!="<class 'bs4.element.Tag'>":
				c=str(c).replace('\xa0','')
			r.append(str(c))
		if k==0:
			ncols=len(r)
		elif len(r)>1:
			r=r[:-2]
		else:
			r+=['']*(ncols-1)
		t.append(r)
		if len(r)!=ncols:
			raise RuntimeError('Inconsistent lenght of rows.')			
		k+=1
	t=np.array(t)

	return t
	
	


def GetDataFromID(ID,Name):
	t=[]
	for corp in [True,False]:
		tbl,webname=GetDataFromID_corp(ID,Name,corp)
		t.append(tbl)
		time.sleep(0+10*np.random.random())
	t=np.concatenate(t,0)
	return 'purehelp.no',t,webname
		
	
	
	
def GetDataFromID_corp(ID,Name,corp):
	if corp:
		url="http://www.purehelp.no/company/corp/"#for corprate numbers
	else:
		url="http://www.purehelp.no/company/account/"
	url=url + str(ID)
	doc=fetchDoc(url)
	if doc==None:
		print ( "Error getting info from %s,%s,corp: %s,%s" %(Name,ID,corp,'purehelp'))
		return [],''
	docStr=read(doc)
	srchstr1='<table(.*?)</table>'
	srchstr2='<tr class(.*?)</tr>'
	srchstr3='(?<=>)([^\<\>\n]+)(?=<)'#Finds all text between '>' and '<' that does not include '>' or '<'. 
	webnamesrchstr='(?<=<title>)([^\<\>\n]+)(?=</title>)'

	tblset,webname=GetTable(url,docStr,srchstr1,srchstr2,srchstr3,webnamesrchstr)
	t=[]
	DescrID=0
	acctypes=['RESULATREGNSKAP','BALANSEREGNSKAP']
	if len(tblset)>0:
		for i in range(2):
			DescrID=LongFormatTable(corp,tblset[i],t,DescrID,acctypes[i])
			DescrID+=1.0
	t=np.array(t)
	return t,webname

def read(doc):
	docStr=doc.read()
	return docStr.decode('utf-8')

def LongFormatTable(corp,tbl,t,DescrID,acctype):
	if len(tbl)==0:
		return []
	if tbl[0][0]!="År":
		raise RuntimeError("ikke år først")	
	yr=tbl[0]
	for i in tbl:
		if i[0]!="År":
			DescrID+=0.01
			if len(i)!=len(yr):
				raise RuntimeError("length problem")
			for j in range(1,len(i)):
				#corp, year ,description,value,DescrID, acctype,currency
				r=[corp,yr[j],i[0],i[j],DescrID,acctype,'NOK']
				t.append(r)
	return DescrID

	
	
def GetTable(url,docStr,srchstr1,srchstr2,srchstr3,webnamesrchstr):
	MandNot=False
	#f=open('docstr.txt','w')
	#f.write(docStr)
	tblstrings=re.findall(srchstr1,docStr)
	webname=re.findall(webnamesrchstr,docStr)[0]
	tblset=[]
	for i in tblstrings:
		rowstrings=re.findall(srchstr2,i)
		tbl=[]
		for j in rowstrings:
			row=re.findall(srchstr3,j)
			r=[]
			for k in row:
				v=convert(k)
				if v!=' ':
					r.append(v)
			if len(r)>1:
				tbl.append(r)
		tblset.append(tbl)
			
	return tblset,webname

def convert(s):
	s=s.replace('POS','')
	s=s.replace('NEG','')
	s=s.replace('.','')
	s=s.replace(',','.')
	isperc= '%' in s
	s=s.replace('%','')
	try:
		return convertperc(int(s),isperc)
	except:
		try:
			return convertperc(float(s),isperc)
		except:
			return s
	
def convertperc(f,isperc):
	if isperc:
		return f/100.0
	else:
		return f
		
	
	
	
def fetchDoc(DocStr):
	c=0
	while True:
		try:
			if c>5:
				return None
			f=urllib.request.urlopen(DocStr)
			break
		except:
			print ( 'waiting ...')
			time.sleep(10+np.random.random()*10)
			c+=1
	return f

def max_year(IsCorporateAccount,OrganizationID,crsr):
	#Retrieves the last year in the database
	SQLExpr="""SELECT distinct
      Year
	  FROM [dbo].[account_brnsnd]
	  where [IsCorporateAccount]='%s' and [OrganizationID]='%s' """ %(IsCorporateAccount,OrganizationID)
	crsr.execute(SQLExpr)
	f=crsr.fetchall()
	r=[]
	for i in range(len(f)):
		if not f[i][0] is None:
			r.append(f[i][0])
	if len(r)==0:
		r=0
	else:
		r=max(r)
	return r

def GetURLFromGoogle(orgnr):
	orgstr=str(orgnr)
	orgstr=orgstr[0:3]+" "+orgstr[3:6]+" "+orgstr[6:9]
	srchstr='"www.proff.no" "Sum salgsinntekter" "Sum+driftsinntekter" "Org nr %s"' %(orgstr,)
	slist=[]
	srch=google.search(srchstr)
	res=None
	for s in srch:
		time.sleep(5+5*np.random.random())
		print ( s)
		if 'regnskap' in s:
			res=s
			break
	return res

def save_to_file():

	t,webName=GetDataFromID(983790739,'Name',None,True,True)
	fu.WriteCSVMatrixFile('account',t,currpath=True)

	pass

columns="""[IsCorporateAccount],[OrganizationID] , [CompanyID],[Name],[webName] ,[FetchDate] ,[Year],[Type] , [Description] ,[Value] ,[DescrID],[Source],[currency]"""
oidlist_sql=f"""
SELECT distinct [CompanyId],[Name],[OrgID] FROM
(SELECT distinct Year([Date]) as [Year] ,[CompanyId]
FROM [OSE].[dbo].[equity]
where Year([Date])>={minyear}) T0
left join
(SELECT distinct [CID],[Name],[OrgID]
  FROM [OSEData].[dbo].[account_OID] ) T1
ON [CID]=[CompanyId]
WHERE (NOT [CompanyId] is Null) and (NOT [OrgID] is Null)
ORDER BY [CompanyId]
"""
GetData()