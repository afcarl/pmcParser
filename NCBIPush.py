import json
import pyodbc
import string
import datetime
from datetime import date
import pickle
import random
import pandas as pd


#Functions to push data to database
#Determining Pull Id
def pull_Id(cnxn):
    query = "select max(PullID) from DataPull_ID"
    pull_Id_Df = pd.read_sql_query(query,cnxn)
    pull_Id_Df.columns = ['maxPullId']
    id_val = pull_Id_Df['maxPullId'].isnull()
    if id_val.bool():
        pull_val = 1
    else:
        pull_id_series = pull_Id_Df['maxPullId'] + 1
        pull_val = pull_id_series.iloc[0]
    return pull_val


def DataPull_ID(cnxn,cur,pull_val,terms,db):
    try:
        pull_id = pull_val.item()
    except:
        pull_id = pull_val

    pull_date = datetime.datetime.now().strftime('%m/%d/%Y %I:%M:%S %p')
    pull_name = terms
    pull_query = terms
    pull_type = 'keyword' #keyword/ author
    pull_source = db.upper()
    pull_by = 'Sophie'

    query = """ insert into DataPull_ID (PullID,PullDate,PullName,PullQuery,PullType,PullSource,PullBy) 
                values (?,?,?,?,?,?,?) """

    args = (pull_id,pull_date,pull_name,pull_query,pull_type,pull_source,pull_by)

    cur.execute(query, args)
    #must commit in order to see it on sql server, if not sql server database won't load correctly
    cnxn.commit()

def DataPull_Detail(cnxn,cur,parse_df,db):
    #populate details table
    # accessPK = cur.execute("select @@IDENTITY").fetchall()[0][0].__str__()
    accessPK = cur.execute("select Dmax('ID','DataPull_ID')").fetchall()[0][0].__str__()
    pullId = cur.execute("select distinct PullID from DataPull_ID where ID = ?", (accessPK)).fetchall()[0][0].__str__()

    aidList = cur.execute("""select distinct b.AssociatedID from DataPull_ID as a inner join DataPull_Detail as b on a.PullID = b.PullID where a.PullSource = ?""",db).fetchall()
    existing_ids = {associatedid[0] for associatedid in aidList}
#     existing_ids

    note = None

    values_list = []

    for index,row in parse_df.iterrows():

        associatedidInt = int(row['associatedId'])
        associatedid = str(associatedidInt)  
        #check if associatedid is already in the database
        if associatedid in existing_ids:
            valuestore = 'duplicate'
            #drop the row that already exists
            parse_df.drop(index, inplace=True)
        else:
            valuestore = 'store'
        for pt in row['pubtype']:
            pub_type_val = pt
            values_list.append((pullId,associatedid,valuestore,pt,note))

    query = """ insert into DataPull_Detail ([PullID],[AssociatedID],[ValueStore],[PubType],[Note]) 
                                values (?,?,?,?,?) """
    if values_list!=[]:
            cur.executemany(query, values_list)
            cnxn.commit()

def DataPull_Title(cnxn,cur,parse_df):
    values_list = []

    for index,row in parse_df.iterrows():
        associatedid = int(row['associatedId'])
        title = row['title']
        journalName = row['journalName']
        journalISO = row['journalISO']
        pubdate = row['publishdatefull']
        try:
            pubDay = row['publishdate']['day']
        except:
            pubDay = None
        try:
            pubMonth = row['publishdate']['month']
        except:
            pubMonth = None
        try:
            pubYear = row['publishdate']['year']
        except:
            pubYear = Non
        optionalId01 = row['optionalId01']
        optionalId02 = row['optionalId02']    

        values_list.append((associatedid, title, journalName, journalISO ,pubdate,pubDay,pubMonth,pubYear, optionalId01,optionalId02))

    query = """ insert into DataPull_Title (AssociatedID, Title, JournalName,JournalISO, PublicationDate, pubDay,pubMonth,pubYear, OptionalID01,OptionalID02) values (?,?,?,?,?,?,?,?,?,?) """

    if values_list!=[]:
        cur.executemany(query, values_list)
        cnxn.commit()


def DataPull_Keyword(cnxn,cur,parse_df):
    values_list = []

    for index,row in parse_df.iterrows():
        associatedid = int(row['associatedId'])
        if row['meshterms'] is not None:
            for word in row['meshterms']:
                keywordvalue = word['descriptorname']
                try:
                    num_of_qualifiers = len(word['qualifiername'])
                except:
                    num_of_qualifiers = 0

                if num_of_qualifiers > 5:
                    num_of_Nones = 0
                    word['qualifiername'] = word['qualifiername'][:5]
                else:
                    num_of_Nones = 5 - num_of_qualifiers

                #if there are qualifier names, the list should not be 0

                if num_of_qualifiers != 0:
                    values_list.append(([associatedid,keywordvalue] + word['qualifiername'] + [None]*num_of_Nones)) 
                else:
                    values_list.append((associatedid, keywordvalue, None, None, None, None, None))

    query = """ insert into DataPull_Keyword (AssociatedID, KeywordValue, Category1, Category2,
                        Category3, Category4, Category5) values (?,?,?,?,?,?,?)"""
    if values_list!=[]:
        cur.executemany(query, values_list)
        cnxn.commit()


def DataPull_Authors(cnxn,cur,parse_df):
    values_list = []

    for index,row in parse_df.iterrows():
        associatedid = int(row['associatedId'])
        auth_count = 0

        if row['author'] is not None:
            for auth in row['author']:
                auth_count += 1
                if auth_count < 4:
                    values_list.append((associatedid, auth['fname'], auth['lname'], auth['affl']))
                else:
                    break

    query = """ insert into DataPull_Author (AssociatedID, ForeName, LastName, Affiliation) values (?,?,?,?) """
    if values_list!=[]:
        cur.executemany(query, values_list)
        cnxn.commit()


def DataPull_Text(cnxn,cur,parse_df):
    values_list = []

    for index,row in parse_df.iterrows():
        associatedid = int(row['associatedId'])
        if row['abstract'] is not None:
            for part in row['abstract']:
                abstracttext = part['text']
                try:
                    label = part['label']
                except:
                    label = None
                try:
                    nlmcategory = part['nlmcategory']
                except:
                    nlmcategory = None
                values_list.append((associatedid,nlmcategory,label,abstracttext))
    query = """ insert into DataPull_Text (associatedid, nlmcategory, label, abstracttext) values (?,?,?,?)"""

    if values_list!=[]:
        cur.executemany(query, values_list)
        cnxn.commit()


#function to save the push id
def to_json(unique_identifier_push):
    json.dump(unique_identifier_push,open("unique_identifier_push.json","w"))


#Import serialized parsed dataframe by it's unique identifier
def import_parse_df(path_name):  
    parse_df_path = path_name
    with open(parse_df_path, 'rb') as f:
        parse_df = pickle.load(f) 
    return parse_df


def create_cnxn(db_name): 
    conn_str = 'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=%s;' % db_name
    cnxn = pyodbc.connect(conn_str)
    cur = cnxn.cursor()
    return cnxn,cur



def main(parsed_path_name,term,db_name,db):
    character_set = string.ascii_letters
    character_set += string.digits
    
    unique_identifier_push = ''
    
    for _ in range(25):
        unique_identifier_push += random.choice(character_set)
      
    #Retrieve parsed dataframe
    parse_df = import_parse_df(parsed_path_name)
    
    #Create connection with database - db is the database we are pushing to
    cnxn,cur = create_cnxn(db_name)
    
    #determine pull_id
    pull_val = pull_Id(cnxn)
    
    #push to DataPull_ID table - db is the db pulled from (PMC or PUBMED)
    DataPull_ID(cnxn,cur,pull_val,term,db)
    
    #push to DataPull_Detail table
    DataPull_Detail(cnxn,cur,parse_df,db)
    
    #push to DataPull_Table table
    DataPull_Title(cnxn,cur,parse_df)
    
    #push to DataPull_Keyword table
    DataPull_Keyword(cnxn,cur,parse_df)
    
    #push to DataPull_Authors    
    DataPull_Authors(cnxn,cur,parse_df)
    
    #push to DataPull_Text Table
    DataPull_Text(cnxn,cur,parse_df)   
    
    return unique_identifier_push, int(pull_val), parse_df.shape[0]


def ex_main_push(parsed_path_name,term,db_name,db):
  unique_identifier_push_list = []
  run_main = main(parsed_path_name,term,db_name,db)
  unique_identifier_push_list.append(run_main[0])
  unique_identifier_push_list.append(run_main[1])
  unique_identifier_push_list.append(run_main[2])
  to_json(unique_identifier_push_list)
  

#Run main and save unique_id and pull id to json
if __name__ == '__main__':
    unique_identifier_fetch = ex_main_push(parsed_path_name,term,db_name,db)  
