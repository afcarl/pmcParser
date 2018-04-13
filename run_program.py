import NCBIFetch
import NCBIPush
import PMCParse
import PUBMEDParse
import json


json_path = "J:/LitReviewTool/ToolandDB/config.json"
config = json.load(open(json_path,"r"))
email_id = config['email']
term = config['query']
db_name = config['db_name']
db = config['db']


def exec_all(term,db_name,db,email_id):
  unique_identifier_fetch = NCBIFetch.ex_main(term,db,db_name,email_id)
  xml_path_name = unique_identifier_fetch + '.pkl'
  if db == 'pmc':
    unique_identifier_parse = PMCParse.ex_main_parse_pmc(xml_path_name)
  elif db == 'pubmed':
    unique_identifier_parse = PUBMEDParse.ex_main_parse_pubmed(xml_path_name)
  elif db not in ['pmc','pubmed']:
    unique_identifier_parse = None
    print('source not recognized')
  parsed_path_name = unique_identifier_parse + '.pkl'
  NCBIPush.ex_main_push(parsed_path_name,term,db_name,db)
  return unique_identifier_fetch,unique_identifier_parse

if __name__ == '__main__':
  unique_identifier_fetch, unique_identifier_parse = exec_all(term,db_name,db,email_id)


print('unique_identifier_fetch: ', unique_identifier_fetch)
print('unique_identifier_parse: ', unique_identifier_parse)
