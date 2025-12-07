import sys, mysql.connector
from functions import import_data, insert_agent_client, add_customized_model, delete_base_model, list_internet_service, count_customized_model, top_N_duration, keyword_search

def printNL2SQLresult():
    import csv
    with open("nl2sql_results.csv") as f:
        reader = csv.DictReader(f)
        for row in reader:
            print(row)

def parse_function(): 
    function = sys.argv[1]
    if (function == "import"):
        import_data(sys.argv[2])
    elif (function == "insertAgentClient"):
        insert_agent_client(int(sys.argv[2]), sys.argv[3], sys.argv[4], int(sys.argv[5]), sys.argv[6], sys.argv[7], int(sys.argv[8]), int(sys.argv[9]), sys.argv[10])
    elif (function == "addCustomizedModel"):
        add_customized_model(int(sys.argv[2]), int(sys.argv[3]))
    elif (function == "deleteBaseModel"):
        delete_base_model(int(sys.argv[2]))
    elif (function == "listInternetService"):
        list_internet_service(int(sys.argv[2]))
    elif (function == "countCustomizedModel"):
        bmid_list = [int(bmid) for bmid in sys.argv[2:]]
        count_customized_model(bmid_list) 
    elif (function == "topNDurationConfig"):
        top_N_duration(int(sys.argv[2]), int(sys.argv[3]))
    elif (function == "listBaseModelKeyWord"):
        keyword_search(sys.argv[2])
    elif function == "printNL2SQLresult":   
        printNL2SQLresult()




if __name__ == "__main__":
    parse_function()