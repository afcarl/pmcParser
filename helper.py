import psycopg2
import string
import random
import pickle
import os
import json
import lxml
import lxml.html
import itertools
from itertools import chain
from itertools import groupby
import datetime

def connect_to_db(db_name):
    #Define our connection string
    conn_string = "host='localhost' dbname=" + db_name + "user='postgres' password='gres'"

    # print the connection string we will use to connect
    print("Connecting to database\n" + conn_string)

    # get a connection, if a connect cannot be made an exception will be raised here
    conn = psycopg2.connect(conn_string)

    # conn.cursor will return a cursor object, you can use this cursor to perform queries
    cursor = conn.cursor()
    return conn, cursor

def run_query(cursor, query):
    query_run = cursor.execute(query)
    query_return = cursor.fetchall()
    return query_return

def create_unique_id():
    character_set = string.ascii_letters
    character_set += string.digits
    unique_identifier = ''
    for _ in range(25):
        unique_identifier += random.choice(character_set)
    return unique_identifier

#serialize id
def serialize_output(unique_id,output):
    # pickle_name = ''
    pickle_name = unique_id + '.pkl'
    with open(pickle_name, 'wb') as f:
      pickle.dump(output, f)

#function to save the unique identifier for the xml as a json
def update_id_json(prop_dict):
    if not os.path.isfile(os.path.join(os.getcwd(),'unique_identifiers.json')):
        with open("unique_identifiers.json", mode='w', encoding='utf-8') as f:
            json.dump([], f)
    if os.path.isfile(os.path.join(os.getcwd(),'unique_identifiers.json')):
        with open("unique_identifiers.json") as f:
            data = json.load(f)
        data.append(prop_dict)
        with open('unique_identifiers.json', 'w') as f:
            json.dump(data, f)

#find most recent run
# def id_run(run_type:str):
def id_run(run_type):
    json_path = "unique_identifiers.json"
    json_file = json.load(open(json_path,"r"))
    type_dict_list = [d for d in json_file if d['id_type'] == run_type]
    most_recent = max(type_dict_list,key = lambda x:x['run_date'])
    unique_id = most_recent['id']
    serialized_data_path = unique_id + '.pkl'
    with open(serialized_data_path, 'rb') as f:
        serialized_data = pickle.load(f)
    return serialized_data

#count number of articles returned from a fetch
def xml_to_html(doc_list,input_db):
  pre_article_list = []
  for doc in doc_list:
    xml = lxml.html.fromstring(doc)
    if input_db == 'pmc':
        article = xml.xpath("//article")
    if input_db == 'pubmed':
        article = xml.xpath("//pubmedarticle")
    pre_article_list.append(article)
    article_list = list(itertools.chain.from_iterable(pre_article_list))
    return article_list

#clean folder
def clean_folder():
  pkl_to_delete = [f for f in os.listdir() if f.endswith('.pkl')]
  uid_to_delete = [i for i in os.listdir() if 'unique_identifier_' in i and i.endswith('.json')]
  to_delete = uid_to_delete+pkl_to_delete
  for f in to_delete:
      os.remove(f)
  # return to_delete




import re
import json
import itertools
from functools import reduce
import collections
from itertools import chain
# from funcy import flatten, isa


#Format quer term
def format_query(input_term):
    i_format = input_term.upper()
    pat = r' AND '
    formatted_terms = [r.replace(" ","+") for r in re.split(pat,i_format)]
    formatted_query = ' AND '.join(formatted_terms)
    return formatted_query

#Format database term
def format_db(input_db):
    formatted_db = input_db.lower()
    return formatted_db

#append query term to query history
def get_history(input_term):
    query_history_full = []
    config = json.load(open("config.json","r"))
    try:
        history = config['query_history']
    except:
        history = config['query'][0]
    query_history_full.append(history)
    query_history_full.append(input_term)
    query_history = list(flatten(query_history_full))
    return query_history


#make dict for configuration file
def make_config_dict(formatted_term,formatted_db,query_history):
    dicter = {}
    dicter['email'] = "s.rand525@gmail.com"
    # dicter['db_name'] = "J:/LitReviewTool/ToolandDB/LitRevDB.accdb"
    dicter['query'] = formatted_term
    dicter['query_history'] = query_history
    dicter['db'] = formatted_db
    return dicter

#create main function
def make_config(input_term, input_db):

    #format latest search term
    formatted_term = format_query(input_term)

    #format latest search term
    formatted_db = format_db(input_db)

    #append last query to history
    try:
      query_history = get_history(formatted_term)
    except:
      query_history = []

    #create dictionary to push to configuation file
    dicter = make_config_dict(formatted_term,formatted_db,query_history)

#     #create json with configuration info
    json.dump(dicter, open("config.json","w"))

    return dicter

# input_term = input("Please enter search term. If multiple search terms, separate by AND/OR: ")
# input_db = input("Which database to search? Please enter Pubmed or PMC. ")

#Run main
if __name__ == '__main__':
    dicter = main(input_term, input_db)





            #Filter record ids based on existence in database
            # def filter_ids(id_list,db_name,db):
            #     conn_str = 'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=%s;' % db_name
            #     cnxn = pyodbc.connect(conn_str)
            #     cur = cnxn.cursor()
            #     aid_list = cur.execute("""select distinct b.AssociatedID from DataPull_ID as a inner join DataPull_Detail as b on a.PullID = b.PullID where a.PullSource = ?""",db).fetchall()
            #     existing_ids = {associatedid[0] for associatedid in aid_list}
            #     id_list_fetch = [fetch_id for fetch_id in id_list if fetch_id not in existing_ids]
            #     return id_list_fetch
