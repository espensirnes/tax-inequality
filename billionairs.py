import importlib
import db
from billionair_fetch import *
if True:
    driver=proff_login()
else:
    driver=None


def main():
    tbl='OID_400'
    conn,crsr=db.Connect(db)
    db.createTable(tbl,conn,crsr,droptable=True)
    columns=f"[{('], ['.join(db.GetColumnNames(crsr,tbl)[0][1:]))}]"
    sqlstr=db.GetSQLInsertStr(tbl,columns)
    namelist=list(pd.read_csv('names.csv',encoding='ANSI',names=['Names']).iloc[:,0])


    importlib.reload(bf)
    k=0
    for name in namelist:
        if (k>=0):# and (orgid in [811176702]):
            print ( 'Appending %s ' %(name,),)
            bf.AppendData(name,conn,crsr,sqlstr,driver)
            print ( ' done')
        k+=1



main()