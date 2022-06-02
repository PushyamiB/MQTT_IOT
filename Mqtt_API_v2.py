import pandas as pd
import json
import pymssql
from flask import Flask,jsonify 
from flask import request 
from flask_cors import CORS
def splitFileIntoListOfDict(filename):
    contents = open(filename, "r").read()
    data = [json.loads(str(item)) for item in contents.strip().split('\n')]
    return data
def connectToDb(details):
    server = details['Server']
    user =  details['User']
    paswd =  details['Password']
    database =  details['Database']
    conn = pymssql.connect(server, user, paswd, database)
    return [conn.cursor(),conn]
def loadJsonData(filename):
    # Opening JSON file
    f = open(filename)
    # returns JSON object as
    # a dictionary
    data = json.load(f)
    # Closing file	
    f.close()
    return data

def post_json_data_validation(json_data ,jsonValidationSchema):
    for k in list(json_data.keys()):
        if(not k in jsonValidationSchema['AcceptableParams']):
            return False
    return True
def form_querry_string(proc_name, json_data):
    querry_list = ['@{}="{}"'.format(k,v) if type(v)==str else '@{}={}'.format(k,v) for k,v in json_data.items()]
    finalQuerry = 'EXEC '+proc_name+" "+','.join(querry_list)
    print("Querry : ",finalQuerry)
    return finalQuerry
def addParamToJsonInDf(df,json_col = 'jsondata',add_col = 'sno',new_key='sno'):
    json_col_data = df[json_col]
#     print(json_col_data)
    json_list = list(json_col_data)
#     print(json_list)
    val_to_insert = list(df[add_col])
    # json_list = list(json_col)
    print(len(json_list),len(val_to_insert))
#         print('str :',isinstanc)
    for i in range(len(json_list)):
#         print('str :',isinstance(json_list[i],str))
        if(isinstance(json_list[i],str)):
            json_list[i]=json_list[i].replace("'",'"')
        try:
            json_list[i]=json.loads(json_list[i])
        except Exception as e:
            print(type(json_list[i]) ," - ",e)
#         print("JSON list : ",json_list[i])
#         print(i,type(val_to_insert[i]))
        if(isinstance(val_to_insert[i],pd._libs.tslibs.timestamps.Timestamp)):
            json_list[i][new_key]=val_to_insert[i].strftime("%m/%d/%Y, %H:%M:%S")
        else:
            json_list[i][new_key]=val_to_insert[i]
#         print("ind_json : ",ind_json)
        df[json_col]=json_list
    return df
if __name__ =='__main__':
    
    jsonValidationSchema = loadJsonData('procedure_schema.json')
    cursor,conn = connectToDb(jsonValidationSchema)
    ##########################FLASK APPLICATIOM#########################
    app = Flask(__name__)
    CORS(app)
    @app.route('/get/api/', methods=['GET', 'POST','DELETE', 'PATCH'])
    def question():
        if request.method == 'POST':
            json_data = request.get_json(force=True)
            json_data = {k:v for k,v in json_data.items() if len(v)>0}
            print(json_data)
            if(post_json_data_validation(json_data ,jsonValidationSchema)):
                querryStr = form_querry_string(jsonValidationSchema['Procedure_name'],json_data)
                print(querryStr)
                cursor.execute(querryStr)
                row = cursor.fetchall()
                conn.commit()
                #df = pd.DataFrame(row, columns =jsonValidationSchema['paramList'])
                #df = addParamToJsonInDf(df.copy(),json_col = 'jsondata',add_col = 'Transdate',new_key='Time')
    #             df.to_excel('dummy_data.xlsx')
    #             string = ""
    #             for i in row:
                    
    #                 string = string + str(i) + "\n"
    # #                 print(i)
                #ret_str = repr(list(df['jsondata']))
                #retDict = {"data" : list(df['jsondata'])}
                stringList = [i[0] for i in row]
                print(stringList)
                return jsonify("".join(stringList))
                #return jsonify(row)
    @app.route('/distinctId/<attribute>',methods=['GET','POST'])
    def distinct_ids(attribute):
      #if request.method=='POST':
        sql_attribute=attribute
        cursor,conn=connectToDb(jsonValidationSchema)
        cursor.execute('Select distinct '+str(sql_attribute)+' from '+str(jsonValidationSchema['Tablename'])+';')
        data=cursor.fetchall()
        conn.commit()
        data_final=[]
        for dist_value in data:
            for final_value in dist_value:
                data_final.append({"id":final_value})
#        data= {"id":final_value for dist_value in data for final_value in dist_value } 
        return jsonify(data_final)            
    

    
    
    
    # app.run(host="0.0.0.0",ssl_context=('/etc/letsencrypt/live/video.bizilliant.com/fullchain.pem', '/etc/letsencrypt/live/video.bizilliant.com/privkey.pem'),port=5002)
    app.run(host="0.0.0.0")
