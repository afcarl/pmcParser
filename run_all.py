import NCBIFetch
import NCBISearch
import PMCParse
import PUBMEDParse
import helper


# input_term = 'public health'
# input_db = 'pmc'
# email_id = 'srand1@health.nyc.gov'


def main_search(input_term,input_db,email_id):

    search_inst = NCBISearch.ncbi_search(input_term,input_db,email_id)

    id_list = search_inst.pub_search()

    prop_dict = search_inst.search_properties()

    unique_id = prop_dict['id']

    helper.serialize_output(unique_id,id_list)

    helper.update_id_json(prop_dict)

def main_fetch(input_db,email_id):

    fetch_inst = NCBIFetch.ncbi_fetch(input_db,email_id)

    id_list_fetch = helper.id_run('search')

    doc_list = fetch_inst.pub_fetch(id_list_fetch)

    prop_dict = fetch_inst.search_properties()

    unique_id = prop_dict['id']

    helper.serialize_output(unique_id,doc_list)

    helper.update_id_json(prop_dict)

def main(input_term,input_db,email_id):
    main_search(input_term,input_db,email_id)
    main_fetch(input_db,email_id)
    if input_db == 'pmc':
        PMCParse.main()
    if input_db == 'pubmed':
        PUBMEDParse.main()

if __name__ == '__main__':
    main(input_term,input_db,email_id)


    #
    # uid_path = "J:/LitReviewTool/ToolandDB/unique_identifier_parse.json"
    # uid_load = json.load(open(uid_path,"r"))
    #
    # json_path = "J:/LitReviewTool/ToolandDB/config.json"
    # config = json.load(open(json_path,"r"))
    # email_id = config['email']
    # term = config['query']
    # db_name = config['db_name']
    # db = config['db']
