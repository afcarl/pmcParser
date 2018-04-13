from Bio import Entrez
import pandas as pd
from pandas import DataFrame
import itertools
from itertools import chain
from itertools import groupby
import codecs
import io
import sys
import encodings
import time
import os
import math
from IPython.display import HTML
from datetime import date
import datetime
import lxml
import lxml.html
import pickle
import types
from operator import itemgetter
import json
import helper
import random
import string
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

class ncbi_fetch:
    def __init__(self,input_db,email_id):
        self.input_db = input_db
        self.email_id = email_id


    #function for fetching xmls for given ids
    def pub_fetch(self,id_list_fetch):
        Entrez.email = self.email_id
        ids = ','.join(id_list_fetch)
        submitinterv = math.ceil(len(id_list_fetch)/10000)
        sleeptime = 5
        restartc = 0
        doc_list = []

        for i in range(submitinterv):
            fetchHandle = Entrez.efetch(db=self.input_db,
                                      retmode='xml',
                                      id=id_list_fetch,
                                      retstart = restartc,
                                      retmax=10000)
            fetchRead = io.TextIOWrapper(fetchHandle.detach(), encoding='utf-8')
            doc = fetchRead.read()
            doc_list.append(doc)
            time.sleep(sleeptime)
            restartc += 10000
        doc_full = doc_list
        # article_list = helper.xml_to_html(doc_full,self.input_db)
        return doc_full

    def search_properties(self):
        unique_id = helper.create_unique_id()
        now = datetime.datetime.now()
        time_stamp = now.strftime("%Y-%m-%d %H:%M")
        id_list = helper.id_run('search')
        run_fetch = self.pub_fetch(id_list)
        article_list = helper.xml_to_html(run_fetch,self.input_db)
        n_records_fetched = len(article_list)
        prop_dict = {'id':unique_id,'id_type':'fetch'
        ,'input_db':self.input_db,'record_count':n_records_fetched
        ,'run_date':time_stamp}
        return prop_dict


#
# #function to serialize the xml doc that was pulled from API
# def serialize_xml(unique_identifier,xml_doc):
#     pklName = unique_identifier+'.pkl'
#     with open(pklName, 'wb') as f:
#       pickle.dump(xml_doc, f)
#
# #function to save the unique identifier for the xml as a json
# def to_json(unique_identifier_fetch):
#     json.dump(unique_identifier_fetch,open("unique_identifier_fetch.json","w"))
#
#
#
# #main function to run all functions and assign unique_identifier to the run
# def main(term,db,db_name,email_id):
#
#     character_set = string.ascii_letters
#     character_set += string.digits
#
#     unique_identifier_fetch = ''
#
#     for _ in range(25):
#         unique_identifier_fetch += random.choice(character_set)
#
#     #Retrieve number of records
#     record_count = pub_count(term,db,email_id)
#
#     #Retrieve record ids
#     id_list = pub_search(term,db,record_count)
#
#     #Filter record ids
#     id_list_fetch = filter_ids(id_list,db_name,db)
#
#     #Fetch xmls for each record id
#     doc_full = pub_fetch(db,id_list_fetch)
#
#     #serialize xml documents (serialize)
#     serialize_xml(unique_identifier_fetch,doc_full)
#
#     return unique_identifier_fetch,id_list_fetch
#
#
# def ex_main(term,db,db_name,email_id):
#   unique_identifier_fetch_list = []
#   run_main = main(term,db,db_name,email_id)
#   unique_identifier_fetch_list.append(run_main[0])
#   unique_identifier_fetch_list.append(len(run_main[1]))
#   to_json(unique_identifier_fetch_list)
#   unique_identifier_fetch = run_main[0]
#   return unique_identifier_fetch
#
#
# if __name__ == '__main__':
#     unique_identifier_fetch = ex_main(term,db,db_name,email_id)
