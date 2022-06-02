import json
import pymssql
import pandas as pd
from IPython.display import display
def loadJsonData(filename):
    # Opening JSON file
    f = open(filename)
    # returns JSON object as
    # a dictionary
    data = json.load(f)
    # Closing file
    f.close()
    return data
def connectToDb(details):
    server = details['Server']
    user = details['User']
    paswd = details['Password']
    database = details['Database']
    conn = pymssql.connect(server, user, paswd, database)
    return [conn.cursor(),conn]
def checkRows(details):
    #cursor.execute('SELECT * FROM INFORMATION_SCHEMA.COLUMNS')
    #cursor.execute(query)
    #row = cursor.fetchall()
    #for i in row:
    #    print(i)
    #conn.commit()
    cursor,conn = connectToDb(details)
    cursor.execute('SELECT * FROM INFORMATION_SCHEMA.COLUMNS')
    row = cursor.fetchall()
    df = pd.DataFrame(row)
    display(df)
    conn.commit()
    attributes=[]
    param_list=[]
    for i,atr in df.iterrows():
        if atr[2]==details['Tablename']:
            param_list.append(atr[3])
            attributes.append({"sql_name":atr[3],"sql_type":atr[7]})
    with open(details['Filename'],'w+') as files:
        details['paramList']=param_list
        details['Attributes']=attributes
        files.write(json.dumps(details))
        files.close()
    return attributes

jsonValidationSchema = loadJsonData('procedure_schema.json')
sqlColumns_dtype=checkRows(jsonValidationSchema)

cursor,conn = connectToDb(jsonValidationSchema)
print("Connected to DB")

data=loadJsonData('procedure_schema.json')
category_type={cat_name['sql_name']:cat_name['sql_type'] for cat_name in data['Attributes'] if cat_name['sql_type'].startswith("varchar")or cat_name['sql_type'].startswith("nvarchar")}
datetime_type={cat_name['sql_name']:cat_name['sql_type'] for cat_name in data['Attributes'] if cat_name['sql_type'].startswith("datetime")}
declare_list=["@{} as {} = null".format(cat_key,cat_value) for cat_key,cat_value in category_type.items()]
body_list=["{}=isnull(@{},{})".format(cat_key,cat_key,cat_key) for cat_key in category_type.keys()]
datetime_dec='@FromDate as datetime = null, @ToDate as datetime = null'
datetime_list=[x for x in datetime_type.keys()]
if data['Procedure']=='create':  
    str1='create procedure '+str(data['Procedure_name'])+'('+(datetime_dec)+" , "+", ".join(declare_list)+')'
else:
    str1='alter PROCEDURE '+str(data['Procedure_name'])+'('+(datetime_dec)+" , "+", ".join(declare_list)+')'
str2='AS BEGIN SELECT * FROM '+str(data['Database'])+".dbo."+str(data['Tablename'])+' where CONVERT(date,{},103) between convert(date,isnull(@FromDate,{}),103) and convert(date,isnull(@ToDate,{}),103)'.format(datetime_list[0],datetime_list[0],datetime_list[0])+" and "+' and '.join(body_list)
str3='order by {} desc for JSON AUTO;'.format(datetime_list[0])+' END ;'
final_str=str1+'\n'+str2+'\n'+str3
print(final_str)
cursor.execute(final_str)
conn.commit()
print("Successuful")
