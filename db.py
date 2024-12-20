#!/usr/bin/python
# -*- coding: UTF-8 -*-

import sys
import pymssql 
import csv
import Functions as fu
import numpy as np
import db_create
import db_indicies

dbase='OSEData'



def connect(db):
	with open('credentials') as f:
		hst, un, pw = f.read().split('\n')[3:]
	conn = pymssql.connect(host=hst, user=un, 
	                       password=pw, database=db)  
	crsr=conn.cursor()
	return conn,crsr

def file_in_db(table,fname,crsr):
	SQLExpr='SELECT [FileName] FROM dbo.%s GROUP BY [FileName]' %(table,)
	crsr.execute(SQLExpr)
	r=crsr.fetchall()
	for i in r:
		if fname==i:
			return True
	return False


def table_exist(db,table,crsr):
	SQLExpr="""SELECT Distinct TABLE_NAME 
                FROM %s.information_schema.TABLES
                where TABLE_NAME='%s'""" %(db,table)
	crsr.execute(SQLExpr)
	r=crsr.fetchall()
	return len(r)==1

def fetch(sqlstr,crsr):
	crsr.execute(sqlstr)
	r=crsr.fetchall()
	return r

def execute(sqlstr,conn,crsr, values = None):
	if values == None:
		crsr.execute(sqlstr)
	else:
		crsr.execute(sqlstr, values)
	conn.commit()

def get_all_tables(db,conn,crsr):
	SQLExpr='SELECT Distinct TABLE_NAME FROM %s.information_schema.TABLES' %(db,)
	crsr.execute(SQLExpr)
	r=crsr.fetchall()
	return r




def deleterows_byfieldval(fieldname,fieldval,tbl,db,conn,crsr):
	SQLExpr="""Delete FROM [%s].dbo.%s
			where [%s] ='%s'""" %(db,tbl,fieldname,fieldval)
	crsr.execute(SQLExpr)
	n=crsr.rowcount
	conn.commit() 
	print ( "%s rows affected by deleting" %(n,))



def get_col_names(crsr,TblName,db=None):
	SQLstr="EXEC sp_columns @table_name = '%s';" %(TblName)
	crsr.execute(SQLstr)
	r=crsr.fetchall()
	if len(r)==0 and not db is None:
		conn,crsr=connect(db)
		crsr.execute(SQLstr)
		r=crsr.fetchall()		
	r=fu.transpose(r)

	return r[3:8]



def insert_into_db(conn,crsr, table,columns,values,tblProps=None):
	columns=col_name_wrapper(columns)
	n=len(columns)
	sstr=',%s'*n
	sstr='('+sstr[1:]+')'
	SQLExpr='INSERT INTO dbo.%s ' %(table,)
	SQLExpr+=sstr %tuple(columns)
	SQLExpr+=' VALUES '+sstr
	return insert_with_column_creation(conn,crsr,table,SQLExpr,columns,values,tblProps)

def insert_table(conn,crsr, table, columns,datatable,db):
	columns=col_name_wrapper(columns)
	n=len(columns)
	sstr=',%s'*n
	sstr='('+sstr[1:]+')'
	SQLExpr='INSERT INTO [%s].dbo.%s ' %(db,table,)
	SQLExpr+=sstr %tuple(columns)
	SQLExpr+=' VALUES '+sstr
	try:
		crsr.executemany(SQLExpr,datatable)	
		conn.commit()
	except Exception as e:
		for i in datatable:
			try:
				crsr.execute(SQLExpr,tuple(i))
			except Exception as e:
				print(i)
				raise RuntimeError(e)
		conn.commit()
	
	#print ( 'Table inserted')
	pass

def col_name_wrapper(columns,smallcaps=False):
	"wraps all column names in []"
	if type(columns)==str:
		columns=fu.Clean(columns)
	n=len(columns)
	columns=list(columns)
	for i in range(n):
		x=columns[i].replace('[','').replace(']','')
		if smallcaps:
			columns[i]='['+x.lower()+']'
		else:
			columns[i]='['+x+']'
	return columns

def insert_with_column_creation(conn,crsr,table,SQLExpr,columns,values,tblProps):
	values=tuple(values)
	err=False
	try:
		crsr.execute(SQLExpr,values)
		conn.commit()
	except pymssql.ProgrammingError as inst:
		err=True
		if  v.args[0]==207:
			tblProps[0]=get_col_names(crsr,table)
			add_columns(conn,crsr,table,columns,tblProps,values)
			try:
				crsr.execute(SQLExpr,values)
				conn.commit()
			except pymssql.OperationalError as inst:
				insert_with_column_extension(conn,crsr,table,columns,tblProps,values,inst,SQLExpr)
		else:
			raise pymssql.ProgrammingError(inst)
	except pymssql.OperationalError as inst:
		err=True
		insert_with_column_extension(conn,crsr,table,columns,tblProps,values,inst,SQLExpr)
	return err


def insert_with_column_extension(conn,crsr,table,columns,tblProps,values,inst,SQLExpr):
	if inst.args[0]==8152:
		tblProps[0]=get_col_names(crsr,table)
		add_columns(conn,crsr,table,columns,tblProps,values)
		tblProps[0]=get_col_names(crsr,table)
		extend_columns(conn,crsr,table,columns,tblProps,values)
		crsr.execute(SQLExpr,values)
		conn.commit()
	elif inst.args[0]==242:
		crsr.execute(SQLExpr,values)
		conn.commit()        
	else:
		raise pymssql.OperationalError(inst)    



def drop_col(tbl,colname,conn,crsr,dbase):

	sqlstr=	"""select 
		dobj.name as def_name
	from sys.columns col 
		left outer join sys.objects dobj 
		    on dobj.object_id = col.default_object_id and dobj.type = 'D' 
	where col.object_id = object_id('[%s].dbo.[%s]')  and col.name='%s'
	and dobj.name is not null""" %(dbase,tbl,colname)
	r=fetch(sqlstr,crsr)
	if len(r)>0:
		for i in r:
			execute('ALTER TABLE [%s].dbo.[%s] DROP CONSTRAINT %s' %(dbase,tbl,i[0]),conn,crsr)
	execute('alter table [%s].dbo.[%s] drop column %s' %(dbase,tbl,colname),conn,crsr)
		
def dbtable_from_csv(fname,tbl_name,conn, crsr, cols, indexfields=None, createnew=True, hasheading=True):
	"""Creates a database table tbl_name from the file given by fname"""
	if createnew:
		drop_table(tbl_name,conn,crsr)
		createTable(tbl_name,conn,crsr,cols)	
	file=open(fname,encoding='latin1')
	k=0
	for r in file.readlines():
		k+=1
		if k>hasheading:
			r=r.replace('\n','')
			r_split=r.split(';')

			r_split.append(fname)
			insert_into_db(conn, crsr, tbl_name, cols, r_split)
	if not indexfields is None:
		create_index(conn, crsr, tbl_name, IndexFields='[CID] ,[Name] ,[OrgID]')

def create_table(tbl,conn,crsr,cols=None,tabledef=None,db=None,droptable=False):
	"""crating a generic table"""
	if droptable:
		drop_table(tbl, conn, crsr,db)
	SQLStr=''
	try:
		if tabledef is None:
			SQLStr=vars(db_create)[tbl]
		else:
			SQLStr=vars(DBCreate)[tabledef]
	except KeyError:
		pass
	if SQLStr=='':
		if not cols is None:
			s='] [varchar](100) NULL,\n['
			s='['+s.join(cols)+s[:-3]+'\n'
			SQLStr="""CREATE TABLE [%s] (ID bigint NOT NULL IDENTITY, \n %s) """ %(tbl,s)
			print(SQLStr)
			raise RuntimeError(f"""Table {tbl} does not exist. Please assign the"""
							   f"""table definition in the above string SQLStr to a new variable you call '{tbl}' in DBCreate""")
		else:
			raise RuntimeError('Table %s does not exist, you need to create it' %(tbl,))			
	crsr.execute(SQLStr)
	conn.commit()
	tblProps=[get_col_names(crsr,tbl,db)]
	add_primary_cey(crsr,conn,tbl)	
	return tblProps[0]

def create_index(conn,crsr,tbl,db=None,createID=False,IndexFields=''):
	if not has_index(tbl,crsr):
		add_primary_cey(crsr,conn,tbl,db,createID)
		if IndexFields=='':
			try:
				IndexFields=vars(DBIndicies)[tbl]
			except KeyError:	
				return
		print ( 'creating index IX_%s ON [%s]' %(tbl,tbl))
		if db is None:
			crsr.execute("""CREATE NONCLUSTERED INDEX IX_%s ON [%s] (%s)""" %(tbl,tbl,IndexFields))
		else:
			crsr.execute("""CREATE NONCLUSTERED INDEX IX_%s ON [%s].[dbo].[%s] (%s)""" %(tbl,db,tbl,IndexFields))
		conn.commit()


def has_index(tbl,crsr):
	SQLstr="""SELECT * 
            FROM sys.indexes 
            WHERE name='IX_%s'""" %(tbl,)
	crsr.execute(SQLstr)
	r=crsr.fetchall()
	return len(r)>0 

def get_sql_insert_str(tbl,columns):
	n=len(columns.split(','))
	sstr=',%s'*n
	sstr='('+sstr[1:]+')'
	SQLExpr='INSERT INTO dbo.%s (%s)' %(tbl,columns)
	SQLExpr+=' VALUES '+sstr
	return SQLExpr

def delete_index(tbl,conn,crsr,db=None):
	if has_index(tbl,crsr):
		print ( 'deleting index IX_%s ON %s' %(tbl,tbl))
		if db is None:
			crsr.execute("""DROP INDEX IX_%s ON [%s]""" %(tbl,tbl))
		else:
			crsr.execute("""DROP INDEX IX_%s ON [%s].[dbo].[%s]""" %(tbl,db,tbl))
		conn.commit()	

def drop_primary_key(crsr,conn,tbl,db=None):
	if db is None:
		SQLStr="""ALTER TABLE [%s] DROP CONSTRAINT PK_%s """ %(db,tbl,tbl)
	else:
		SQLStr="""ALTER TABLE [%s].[dbo].[%s] DROP CONSTRAINT PK_%s """ %(db,tbl,tbl)
	crsr.execute(SQLStr)
	conn.commit()	


def add_primary_cey(crsr,conn,tbl,db=None,createID=False):
	if createID:
		try:
			if db is None:
				crsr.execute("""ALTER TABLE [%s] ADD ID INT IDENTITY""" %(db,tbl))
			else:
				crsr.execute("""ALTER TABLE [%s].[dbo].[%s] ADD ID INT IDENTITY""" %(tbl))
			conn.commit()
		except:
			pass
	try:
		if db is None:
			crsr.execute("""ALTER TABLE [%s] ADD CONSTRAINT
				PK_%s PRIMARY KEY CLUSTERED (ID)""" %(tbl,tbl))
		else:
			crsr.execute("""ALTER TABLE [%s].[dbo].[%s] ADD CONSTRAINT
					    PK_%s PRIMARY KEY CLUSTERED (ID)""" %(db,tbl,tbl))			
		conn.commit()	
	except:
		pass

def add_columns(conn,crsr,table,columns,tblProps,values=None):
	existingcols=tblProps[0][0]
	existingcols=col_name_wrapper(existingcols,True)
	columns=col_name_wrapper(columns)
	n=len(columns)
	for i in range(n):
		if not(columns[i].lower() in existingcols):
			if not values is None:
				add_column(crsr,conn,table,columns[i],max(2*len(values[i]),10))
			else:
				add_column(crsr,conn,table,columns[i],20)




def add_column(crsr,conn,table,column,length):
	if table=='newsdump' and column=='[text]':
		SQLstr="""ALTER TABLE %s ADD %s varchar(max) DEFAULT NULL""" %(table,column)
	else:
		SQLstr="""ALTER TABLE %s ADD %s varchar(%s) DEFAULT NULL""" %(table,column,max((length,1)))
	crsr.execute(SQLstr)
	conn.commit()


def extend_columns(conn,crsr,table,columns,tblProps,values):
	columns=col_name_wrapper(columns)
	lenghts=tblProps[0][3]
	keys=col_name_wrapper(tblProps[0][0])
	existingdict = dict(zip(keys, lenghts))
	n=len(columns)
	for i in range(n):
		if existingdict[columns[i]]<len(values[i]):
			extend_column(crsr,conn,table,columns[i],values[i])



def extend_column(crsr,conn,table,column,value):
	if table=='newsdump' and column=='[text]':
		SQLstr="""ALTER TABLE %s ALTER COLUMN %s varchar(max)""" %(table,column)
	else:
		SQLstr="""ALTER TABLE %s ALTER COLUMN %s varchar(%s)""" %(table,column,max(2*len(value),10))
	crsr.execute(SQLstr)
	conn.commit()


def drop_table(table,conn,crsr,db=None):
	"Deletes a table"
	try:
		if db is None:
			crsr.execute("DROP TABLE [%s];" %(table))
		else:
			crsr.execute("DROP TABLE [%s].[dbo].[%s];" %(db,table))
		conn.commit()
	except:
		pass



def copy_table(conn,crsr,fromtbl,totbl,fromdb,todb=None):
	if todb is None:
		todb=fromdb
	sqlstr="""SELECT * INTO [%s].[dbo].[%s]
            FROM [%s].[dbo].[%s]""" %(todb,totbl,fromdb,fromtbl)
	crsr.execute(sqlstr)
	conn.commit()

	
