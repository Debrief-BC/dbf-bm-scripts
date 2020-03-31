# using json rpc for sending transactions
#!/usr/bin/env python3
import requests
import json
import os
import sys
import string
import multiprocessing
import time
from joblib import Parallel, delayed

#default for testing
#to_addr="0x0c5a..."
value_tr="0x9184e72a" #tx
os.environ['NO_PROXY'] = '127.0.0.1'

session = requests.Session()

with open("config.json",'r') as f:  #Grab from config
    data = json.load(f)
from_add=data['wallet']['from']
to_addr=data['wallet']['to']
ip_address =data['server']['address']
password = data['wallet']['password']

def unlock_account (time):					#Opens and closes - Now with idea of rate can keep open
    data='{"method":"personal_unlockAccount","params":["'+from_add+'","'+password+'",null],"id":1,"jsonrpc":"2.0"}'
    headers =  {'Content-type': 'application/json'}
    r = session.post(ip_address,data=data,headers=headers)


def compose_transactionJSONRPC(t_from,t_to,t_value,nonce):
    params = '{"from": "'+t_from+'", "to": "'+t_to+'", "value": "'+t_value+'", "nonce": "'+hex(nonce)+'"}'
    data='{"method":"parity_composeTransaction", "params":['+params+'],"id":1,"jsonrpc":"2.0"}'
    headers =  {'Content-type': 'application/json'}
    r = session.post(ip_address,data=data,headers=headers)    #Session setup, to avoid excessive Sessions
    return r.json()['result']

def sign_transactionJSONRPC(object):
    del object["condition"]
    obj_toString = str(object)
    obj_toString = obj_toString.replace("'",'"')
    data='{"method":"eth_signTransaction", "params":['+obj_toString+'],"id":1,"jsonrpc":"2.0"}'
    headers =  {'Content-type': 'application/json'}
    r = session.post(ip_address,data=data,headers=headers)
    return r.json()['result']['raw']

def send_transactionJSONRPC(object):
    obj_toString = str(object)
    obj_toString = obj_toString.replace("'",'"')
    data='{"method":"eth_sendRawTransaction", "params":["'+obj_toString+'"],"id":1,"jsonrpc":"2.0"}'
    headers =  {'Content-type': 'application/json'}
    r = session.post(ip_address,data=data,headers=headers)
    return r.json()['result']

def generateMassTransactions (rate,lengthinseconds):		#Old Ver
    total_transactions = rate*lengthinseconds
    signed_arr =[]
    print("Creating "+ str(total_transactions)+ " signed transactions")
    #find next nonce
    data='{"method":"parity_nextNonce","params":["'+from_add+'"],"id":1,"jsonrpc":"2.0"}'
    headers =  {'Content-type': 'application/json'}
    r = requests.post(ip_address,data=data,headers=headers)
    print(r.json()['result'])
    next_nonce = int(r.json()['result'],0)
    for x in range (0,total_transactions):
        unlock_account(0)
        new_tran=compose_transactionJSONRPC(from_add,to_addr,value_tr,x+next_nonce)
        raw_tran = sign_transactionJSONRPC(new_tran)
        if x % 100 == 0 :
            print("Its is " + str((x/total_transactions)*100) + "% done")
        signed_arr.append(raw_tran)
    return signed_arr

def newGenerateMassTransactions (rate,lengthinseconds):  #Parallelized!
    total_transactions = rate*lengthinseconds
    signed_arr =[]
    print("Creating "+ str(total_transactions)+ " signed transactions")
    #find next nonce
    num_cores = multiprocessing.cpu_count()
    data='{"method":"parity_nextNonce","params":["'+from_add+'"],"id":1,"jsonrpc":"2.0"}'
    headers =  {'Content-type': 'application/json'}
    r = requests.post(ip_address,data=data,headers=headers)
    print(r.json()['result'])
    next_nonce = int(r.json()['result'],0)
    signed_arr = Parallel(n_jobs = num_cores)(delayed(generateSingleSignedTransaction)(x,next_nonce,total_transactions) for x in range(0,total_transactions))
    return signed_arr

def generateSingleSignedTransaction (x,next_nonce,total_transactions):
    unlock_account(0)
    new_tran=compose_transactionJSONRPC(from_add,to_addr,value_tr,x+next_nonce)
    raw_tran = sign_transactionJSONRPC(new_tran)
    if x % 100 == 0 :
        print("Its is " + str((x/total_transactions)*100) + "% done")
    return(raw_tran)

def getBlockNumberJSONRPC():			
    data='{"method":"eth_blockNumber", "params":[],"id":1,"jsonrpc":"2.0"}'
    headers =  {'Content-type': 'application/json'}
    r = requests.post(ip_address,data=data,headers=headers)
    return r.json()['result']

def recordBlockTransactionNumbers(block_num):
    data='{"method":"eth_getBlockTransactionCountByNumber", "params":["'+hex(block_num)+'"],"id":1,"jsonrpc":"2.0"}'
    headers =  {'Content-type': 'application/json'}
    r = requests.post(ip_address,data=data,headers=headers)
    return r.json()['result']

def getTransactionReceipt (hash):
    data='{"method":"eth_getTransactionReceipt", "params":["'+hash+'"],"id":1,"jsonrpc":"2.0"}'
    headers =  {'Content-type': 'application/json'}
    r = requests.post(ip_address,data=data,headers=headers)
    return r.json()['result']['blockNumber']

if __name__ == "__main__":
    print("Type in the number of transactions per second:")
    perSecond = input("Rate: ")
    print("How many Seconds should it run for:")
    time_length = input("Seconds: ")
    transaction_array = newGenerateMassTransactions(int(perSecond),int(time_length))
    num_cores = multiprocessing.cpu_count()
    print("Generated "+str(len(transaction_array)) + " transactions. Now sending")
    #get initial eth_blockNumber
    input("Press Enter to continue")
    initial_block = int(getBlockNumberJSONRPC(),0)
    print("Initial block: " + str(initial_block))
    all_results=[]
    for x in range(0,int(time_length)):
        grab_rate = transaction_array[x*int(perSecond):(x*int(perSecond))+int(perSecond)]
        results =(Parallel(n_jobs = num_cores)(delayed(send_transactionJSONRPC)(transaction) for transaction in grab_rate))
        time.sleep(1)
        all_results+=results
    #results = Parallel(n_jobs = num_cores)(delayed(send_transactionJSONRPC)(transaction) for transaction in transaction_array)
    last_transaction_hash = all_results[len(all_results)-1]
    first_transaction_hash = all_results[0]
    print("first_transaction_hash: "+first_transaction_hash)
    print("last_transaction_hash: "+last_transaction_hash)
    current_time =int(time.time())
    block_csv = "block_test"+str(current_time)+".csv"
    with open(block_csv,"a+") as f:
        f.write("first_transaction_hash, last_transaction_hash")
        f.write("\n"+first_transaction_hash+", "+last_transaction_hash)
    print("Recording block info in file: " + block_csv)
    print("All done")
