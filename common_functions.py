import requests
import numpy as np
import pandas as pd

from datetime import datetime, timedelta
from colorama import Fore
from London import London

from conf import *


def reset():
    action = "reset"
    port = 5000
    url = 'http://{}:{}/{}'.format(host, port, action)
    
    response = requests.get(url)
    
    if response.status_code != 200:
        print('{} ERROR on mining {}'.format(Fore.RED, Fore.RESET))
    
    London.reset(London)

def getClusterBuildings(clusters=7):
    from pi import pi
    from textwrap import wrap
    
    buildings = wrap(pi, 2)
    buildings = buildings[0:clusters]

    return list(map(int, buildings))

def is_port_in_use(host, port):
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex((host, port)) == 0

def create_server(host, threads, port=5000):
    import threading
    from blockchain import blockchain
    
    # set up the server
    while is_port_in_use(host, port):
        port += 1
    t = threading.Thread(target=blockchain.new_server, args=(host, port,))
    threads.append(t)
    t.start()
    return threads


def create_neighbours(host, neighbours, threads):
    import threading
    from blockchain_client import blockchain_client
    
    # set up the clients
    neigbour = 0
    while neigbour < neighbours:
        port = 8080 + PULL_PORTS.pop(0)
        if not is_port_in_use(host, port):
            t = threading.Thread(target=blockchain_client.new_client, args=(host, port,))
            t.start()
            threads.append(t)
            neigbour += 1
    
    return threads


def setThreadsNeighbours(host, neighbours):
    import time
    
    threads = list()
    
    threads = create_neighbours(host, neighbours, threads)
    threads = create_server(host, threads)
    # wait 5 seconds until all clients and the server are set up
    time.sleep(5)
    
    return threads


def get_keys(host, port):
    
    # We can use integers directly instead of the pk generated from RSA algorithm to easily see who is giving what
    action = "wallet/new"
    url = 'http://{}:{}/{}'.format(host, port, action)
    
    response = requests.get(url)
    
    if response.status_code == 200:
        pk = response.json()['public_key']
        sk = response.json()['private_key']
        return {"sk": sk, "pk": pk}


def mine(host):
    
    action = "mine"
    port = 5000
    url = 'http://{}:{}/{}'.format(host, port, action)
    
    response = requests.get(url)
    
    if response.status_code != 200:
        print('{} ERROR on mining {}'.format(Fore.RED, Fore.RESET))


def initialise(neighbours, threads):
    neighbours_data = {}
    
    for neighbour in range(neighbours):
        port = threads[neighbour]._args[1]
        aux = get_keys(host, port)
        aux['port'] = port
        aux['neighbour'] = London(start_date, end_date)
        neighbours_data[neighbour] = aux
    return neighbours_data


def is_end_month(time):
    current_time = start_date + timedelta(minutes=time * 30)
    
    end = False
    if current_time.day == end_month.day \
            and current_time.hour == end_month.hour \
            and current_time.minute == end_month.minute:
        end = True
    
    return end


def generate_new_threshold(date_aux):
    # TODO: Here we check whether we have to generate a new threshold, that is, if the billing algorithm
    # has been computed. This is not a requirement since the threshold can be calculated independendlty
    
    current_time = start_date + timedelta(minutes=date_aux * 30)
    aux = pd.Timestamp(current_time)
    if aux.isoweekday() == 1 and aux.hour==0 and aux.minute==0:
        return aux, aux+pd.DateOffset(7)
    else:
        return False
    

def make_transaction(host, data, tran):
    action = "generate/transaction"
    
    url = 'http://{}:{}/{}'.format(host, data[tran[0][0]]['port'], action)
    
    payload = {"sender_address": data[tran[0][0]]['pk'],
               "sender_private_key": data[tran[0][0]]['sk'],
               "recipient_address": data[tran[0][1]]['pk'],
               "amount": tran[1]}
    
    response = requests.post(url, data=payload)
    
    if response.status_code == 200:
        # transaction = response.json()['transaction']
        # signature = response.json()['signature']
        
        del (payload['sender_private_key'])
        payload['signature'] = response.json()['signature']
        action = "transactions/new"
        port_server = 5000
        url = 'http://{}:{}/{}'.format(host, port_server, action)
        
        requests.post(url, data=payload)
        
        if response.status_code != 200:
            print('{} ERROR in make transaction {}'.format(Fore.RED, Fore.RESET))
    else:
        print('{} ERROR in query: make_transaction {}'.format(Fore.RED, Fore.RESET))


def who_can_share(can_take_electricity, consumptions_aux, means, owe):
    # Selfish attacker (1): Always want to share
    if can_take_electricity.size:
        can_share_electricity = consumptions_aux[~ np.isin(consumptions_aux[:, 0], can_take_electricity[:, 0])]
    else:
        can_share_electricity = np.copy(consumptions_aux)
    
    means_aux = means[:, 1] * 2
    to_delete = np.ones(can_share_electricity.shape[0], dtype=bool)
    
    for ind, elem in enumerate(list(can_share_electricity)):
        if int(elem[0]) in owe.keys():
            for aux in owe[int(elem[0])]:
                if owe[int(elem[0])][aux] > means_aux[int(elem[0])]:
                    to_delete[ind] = False
    
    can_share_electricity = can_share_electricity[to_delete]
    
    return can_share_electricity


def who_can_take(neighbours, means, owe):
    output = []
    for neighbour in neighbours:
        if neighbour[1] < means[int(neighbour[0])][1]:
            output.append(neighbour)
    return np.array(output)


def add_transaction(from_, to_, value, transactions):
    index_aux = [from_, to_]
    added = False
    for cont in range(len(transactions)):
        if transactions[cont][0] == index_aux:
            transactions[cont][1] += value
            added = True
    if not added:
        transactions.append([index_aux, value])
    
    return transactions


def get_all_debts(data, block_number):
    # /transactions/get
    action = "chain"
    port = 5000
    
    pk = []
    debts = {}
    
    for elem in data.values():
        pk.append(elem['pk'])
    
    url = 'http://{}:{}/{}'.format(host, port, action)
    
    response = requests.get(url)
    
    if response.status_code == 200:
        chains = response.json()['chain']
        for chain in chains:
            if block_number < chain['block_number']:
                block_number = chain['block_number']
                if debug:
                    print('*' * 10)
                for transaction in chain['transactions']:
                    
                    if transaction['recipient_address'] in pk:
                        recipient = pk.index(transaction['recipient_address'])
                        sender = pk.index(transaction['sender_address'])
                        value = float(transaction['value'])
                        
                        if debug:
                            print('neighbour: {} \t owes: {} \t to neighbour: {}'.format(sender, value, recipient))
                        
                        if value:
                            if sender not in debts.keys():
                                debts[sender] = {}
                            
                            if recipient not in debts[sender].keys():
                                debts[sender][recipient] = value
                            else:
                                debts[sender][recipient] += value
                            
                            debts[sender][recipient] = np.around(debts[sender][recipient], decimals=3)

                            # TODO: Implement a pretty way to solve useless the debts
                            if recipient in debts.keys():
                                if sender in debts[recipient].keys():
                                    if debts[sender][recipient] - debts[recipient][sender] < 0:
                                        if debts[recipient][sender] - debts[sender][recipient] < 0:
                                            pass
                                        else:
                                            pass
                                    else:
                                        pass
                            # TODO: End Implement a pretty way to solve useless the debts
                            
                            if debts[sender][recipient] == 0.0:
                                del (debts[sender][recipient])
                                if len(debts[sender]) == 0:
                                    del (debts[sender])
    
    return debts, block_number


def get_threshold(neighbours_data):
    # Statistically predict the consumption based on previous week|days|patterns etc
    # TODO: For real deployment, use previous data. For testing we assume the prediction is perfect
    means = []
    for key, value in neighbours_data.items():
        means.append([key, value['neighbour'].get_consumption_range(start_date, end_date)])
    
    return means