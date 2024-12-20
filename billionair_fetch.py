#!/usr/bin/python
# -*- coding: UTF-8 -*-

#A module that fetch accouting data from the net

import sys
sys.path.append('../../')
import urllib.request
import re
import datetime as dt
import csv
#import google
import time
import numpy as np
import datetime
import db
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium import common
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import hashlib
from selenium.webdriver.common.by import By


DBNAME='research'
sleeptime=2

def get_data():
	tbl='OID_400'
	tbl_ownership = 'ownership'
	conn,crsr=db.connect(DBNAME)
	db.create_table(tbl,conn,crsr,droptable=True)
	db.create_table(tbl_ownership,conn,crsr,droptable=True)

	namelist=list(pd.read_csv('names.csv',encoding='ANSI',names=['Names']).iloc[:,0])
	
	driver=proff_login()


	k=0
	for name in namelist:
		if (k>=0):# and (orgid in [811176702]):
			print ( 'Appending %s ' %(name,),)
			appen_data(name,conn,crsr,driver)
			print ( ' done')
		k+=1
		
	db.create_index(conn,crsr,tbl,DBNAME)
	
	
def proff_login():
	url="https://www.forvalt.no/"
	path = ChromeDriverManager().install()
	driver = webdriver.Chrome(service=webdriver.ChromeService(path))
	driver.get(url)
	login =driver.find_elements(By.ID,'loginBox')
	inputs=login[0].find_elements(By.TAG_NAME,'input')
	with open('credentials') as f:
		un, pw = f.read().split('\n')[:2]
	inputs[0].send_keys(un)
	inputs[1].send_keys(pw)
	return driver
	

def sleep():
	time.sleep(sleeptime +2*np.random.random())
	return

	now=datetime.datetime.now()
	wakeup=datetime.datetime(now.year,now.month,now.day,8,3,27)+datetime.timedelta(days=1)
	gohome=datetime.datetime(now.year,now.month,now.day,22,58,36)				
	
	while now>gohome and now<wakeup:
		time.sleep(1000)
		now=datetime.datetime.now()

	time.sleep(sleeptime +2*np.random.random())

	
	
	
def appen_data(name,conn,crsr,driver):
	
	pid,idname=get_person_id(driver,name)
	if pid is None:
		add_to_db(crsr, conn, name)
		return
	comp_table=get_companies(driver,pid,crsr)
	ownership=[]

	for c_name, oid, perc in comp_table:
		owned_by, owning, desc = get_ownership(driver, oid, crsr, name)
		ownership.append([owned_by, owning, desc,c_name, oid, perc])
	if len(ownership)==0:
		add_to_db(crsr, conn, name)
		return
	
	added=[]

	for owned_by, owning, desc, c_name, oid, perc  in ownership:
		oid_path=[pid]
		perc_path=[perc]			
		added.append(
			get_company(driver,oid,oid_path,perc_path,conn,crsr,
								 name,c_name, perc, True, ownership=(owned_by, owning, desc)
								 )
					 )	


	
def get_person_id(driver,owner_name):
	url=(f"https://www.forvalt.no/ForetaksIndex/StackedResult?SearchForm.SingleFieldQueryString={owner_name.replace(' ','+')}"
	f"&SearchForm.RunPersonSearch=true&SearchForm.RunCompanySearch=false&SearchForm.RunBranchSearch=false")	
	tables=get_tables(driver, url)
	if len(tables)==0:
		return None,None
	table=tables[0]
	pid = []
	verv= []
	name=[]
	for table_row in table.findAll('tr'):
		columns = table_row.findAll('td')
		if len(columns)>0:
			pid.append(columns[0].find('a')['href'].split('/')[-1])
			verv.append(int(columns[-1].text.split(',')[-1].replace(' verv','')))
			name.append(columns[0].text)
		
	top=np.array(verv).argsort()[-1]
	if name[top]!=owner_name:
		return None, None
	return pid[top],name[top]
	
def get_companies(driver,pid,crsr):
	url=f"https://www.forvalt.no/ForetaksIndex/RollePerson/{pid}"
	table=get_tables(driver,url)[3]
	t = get_tables(driver,url, True)
	comp_table=[]
	for table_row in table.findAll('tr'):
		columns = table_row.findAll('td')
		if len(columns)>0:
			name = columns[0].text.replace('\n','').replace('\xa0', '')
			perc = columns[3].text.replace('\n','').strip().replace('%','')
			comp_table.append([
				name[:name.find('Org.nr')].strip(),
				columns[0].find('a')['href'].split('/')[-1], 
				float(perc.replace(',','.'))/100
				])
	return comp_table

def get_company(driver,oid,oid_path,perc_path,conn,crsr,
				owner_name,comp_name,perc, directly_owned, ownership=None
				, ):
	
	if ownership is None:
		owned_by, owning, desc = get_ownership(driver, oid, crsr, owner_name)
	else:
		owned_by, owning, desc = ownership
		
	if owned_by is None:
		return False


	print(f"Adding {desc['Selskapsnavn']}({owner_name})")
	a=0

	add_to_db(crsr, conn, owner_name, desc['Selskapsnavn'],
			   oid, oid_path, perc_path, directly_owned, 
			   owned_by, owning, desc)


	if owning is None:
		return True
	
	oid_path = list(oid_path)
	perc_path = list(perc_path)
	oid_path.append(oid)
	perc_path.append(perc)
	for name,stocks,perc,oid,firm in owning:
		get_company(driver,oid,oid_path,perc_path,conn,crsr,owner_name,name,perc, False)
		
	return True


def get_ownership(driver,oid,crsr,owner_name):

	ownership = ownership_from_db(crsr, oid)
	if not ownership is None:
		owned_by, owning, desc  = [eval(i) for i in ownership]
		return owned_by, owning, desc 
		
	url=f"https://www.forvalt.no/ForetaksIndex/Firma/FirmaSide/{oid}"
	t=get_tables(driver,url,True)

	desc={i[0]:i[1] for i in t[0]}
	if not desc['Organisasjonsform'] in ['Allmennaksjeselskap (ASA)', 'Aksjeselskap (AS)']:
		return [],[], []
			
	owned_by, owning = [],[]
	for i in t:
		if i[0] in [['Org.nr.', 'Selskapsnavn', 'Andel'],['Selskapsnavn', 'Ant. aksjer ', 'Andel']]:
			owning.extend(convert_owning(i[1:]))
		elif i[0]==['Navn', 'Ant. aksjer', 'Andel']:
			owned_by=i[1:]	
	
	assert not (owned_by is None)

	return owned_by, owning, desc

def convert_owning(owning):
	if len(owning[0])>3:
		return owning
	t=[]
	for oid,name,perc in owning:
		perc = float(perc.replace('%','').replace('.',''))/100
		t.append([name,'',perc,oid,'FirmaSide'])
	return t

		
def get_tables(driver,url,return_as_arrays=False):
	try:
		driver.get(url)
	except common.TimeoutException:
		print('Timeout exception')
		driver.get(url)
	sleep()
	html=driver.page_source
	bs=BeautifulSoup(html,"lxml")
	tables=bs.find_all('table')
	if not return_as_arrays:
		return tables
	t=[]
	for i in tables:
		t.append(get_table(i))	
	return t

def get_table(table):
	t=[]
	for j in table.findAll('tr'):
		tr=[]
		tr_full=[]
		for r in j.findAll('th')+j.findAll('td'):
			tr.append(r.text.replace('  ','').replace('\n','').replace('\xa0860','').replace('\xa0780',''))
			tr_full.append(r)
		insert_oid_in_owned(t, tr_full, tr)
		t.append(tr)

	return t

def insert_oid_in_owned(t,r,tr):
	if len(t)==0:
		return	
	if t[0]==['Org.nr.', 'Selskapsnavn', 'Andel']:
		tr[2]=tr[2].replace('%','').replace(',','.')
		return
	if not t[0] in [['Navn', 'Ant. aksjer', 'Andel'],['Selskapsnavn', 'Ant. aksjer ', 'Andel']]:
		return


			
	a=r[0].find('a')
	if a is None:a=[]
	if len(a):
		a=a['href'].split('/')
		tr.append(a[-1])
		tr.append(a[-2])
	else:
		tr.append('')
		tr.append('')
	tr[2]=tr[2].replace('%','').replace(',','.')

def add_to_db(crsr, conn, owner_name, comp_name = None, oid = None, 
			  oid_path = None, perc_path = None,  directly_owned = None, 
			  owned_by = None, owning = None, desc = None):
	
	
	INSERT_STR_OID400 = "INSERT INTO [research].[dbo].[OID_400] ([OwnerName], [CompName], [OrganizationID], [DirectlyOwned], [owned_by], [owning], [desc], [hash]) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
	INSERT_STR_OWNER = 'INSERT INTO [research].[dbo].[ownership] ([OrganizationID], [OwnerOID], [perc], [level], [hash]) VALUES (%s,%s,%s,%s,%s)'
	
	if oid is None:
		if not exist(crsr,owner_name = owner_name):
			db.execute(INSERT_STR_OID400, conn, crsr,(owner_name, None, None, None, None, None, None, None))
		return
	
	dbhash = calculate_hash(oid_path)
	
	if exist(crsr, oid, dbhash):
		return False
	
	db.execute(INSERT_STR_OID400, conn, crsr,(owner_name, comp_name, oid, directly_owned, str(owned_by), str(owning), str(desc), dbhash))
	
	n = len(oid_path)
	oid_path = oid_path[::-1]
	perc_path = perc_path[::-1]
	
	
	for i in range(n):
		db.execute(INSERT_STR_OWNER, conn, crsr,(oid, oid_path[i], perc_path[i], i, dbhash))
	conn.commit()
	
	return True


def exist(crsr,oid = None, dbhash=None, owner_name = None):
	
	sqlstr = f"""
	SELECT DISTINCT [OrganizationID]
		FROM [research].[dbo].[OID_400]
		WHERE [OrganizationID] = {oid}
		AND [hash] = '{dbhash}'
		"""
	if not owner_name is None:
		sqlstr = f"""
		SELECT DISTINCT [OrganizationID]
			FROM [research].[dbo].[OID_400]
			WHERE [OwnerName] = '{owner_name}'
			"""
		
	r=db.fetch(sqlstr, crsr)
	return len(r)>0
			
def ownership_from_db(crsr,oid):

	sqlstr = f"""
	SELECT DISTINCT [OrganizationID], [owned_by], [owning], [desc]
		FROM [research].[dbo].[OID_400]
		WHERE [OrganizationID] = {oid}
		"""

	r=db.fetch(sqlstr, crsr)
	if len(r)>0:
		owned_by, owning, desc = r[0][1:]
		return owned_by, owning, desc
	else:
		return None



def calculate_hash(oid_path):
	if oid_path is None:
		return 0
	concatenated_ids = '/'.join(oid_path)
	sha256 = hashlib.sha256()
	sha256.update(concatenated_ids.encode('utf-8'))
	return sha256.hexdigest()

get_data()