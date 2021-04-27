import logging, os

from common_functions import *
from tqdm import tqdm

logging.basicConfig()
logging.getLogger().setLevel(logging.CRITICAL)
requests_log = logging.getLogger("werkzeug")
requests_log.setLevel(logging.CRITICAL)
requests_log.propagate = False

    
def lightweight_solution_week_avg(data, time, owe, means):
    consumptions = []
    transactions = []
    
    means = np.array(means)
    
    for key, value in data.items():
        consumptions.append([key, value['neighbour'].get_consumption(time)])
    
    for key, value in list(owe.items()):
        for key2, value2 in list(value.items()):
            if is_end_month(time):
                consumptions[key][1] += value2
                consumptions[key2][1] -= value2
                transactions.append([[key2, key], np.around(value2,decimals=3)])
                del(owe[key][key2])
                if not owe[key]:
                    del owe[key]

    
    consumptions_aux = np.asarray(consumptions)
    
    if not is_end_month(time):
        can_take_electricity = who_can_take(consumptions_aux, means, owe)
        can_share_electricity = who_can_share(can_take_electricity, consumptions_aux, means, owe)
        
        # Shuffle all the elements to avoid always the same people share/consume
        for aux in range(5):
            np.random.shuffle(can_take_electricity)
            np.random.shuffle(can_share_electricity)
        
        for neigh in can_share_electricity:
            value_to_share = np.abs(means[int(neigh[0])][1] - neigh[1])
            
            for neigh_can_take in list(can_take_electricity):
                consumption = neigh_can_take[1]
                means_aux = means[int(neigh_can_take[0])][1]
                current_consumption_left = means_aux - consumption

                if (value_to_share - current_consumption_left) <= 0:
                    consumption_taken = value_to_share
                else:
                    consumption_taken = current_consumption_left
                    can_take_electricity = np.delete(can_take_electricity,0,0)

                value_to_share -= consumption_taken
                transactions = add_transaction(int(neigh[0]), int(neigh_can_take[0]),
                                               np.around(consumption_taken,decimals=3), transactions)

                consumptions_aux[np.where(consumptions_aux[:,0]==neigh[0])[0][0]][1] -= consumption_taken
                consumptions_aux[np.where(consumptions_aux[:,0]==neigh_can_take[0])[0][0]][1] += consumption_taken
                
        # current consumption is below the mean == can take consumption from someone else
        # so this guy will try to solve debts (if any)
        for neigh_can_take in list(can_take_electricity):
            if int(neigh_can_take[0]) in owe.keys():
                # I owe consumption. Let's try to give it back
                a_deber = list(owe[int(neigh_can_take[0])].keys())
    
                deudor = int(neigh_can_take[0])
                amount_can_share = means[deudor][1] - consumptions_aux[deudor][1]
    
                for a_deb in a_deber:
                    if consumptions_aux[a_deb][1]>0:
                        if amount_can_share > 0:
                            if ((amount_can_share - owe[deudor][a_deb]) >= 0):
                                amount_can_share -= owe[deudor][a_deb]
                                consumptions_aux[a_deb][1] -= owe[deudor][a_deb]
                                consumptions_aux[deudor][1] += owe[deudor][a_deb]
                                transactions = add_transaction(a_deb, deudor, np.around(owe[deudor][a_deb],decimals=3), transactions)
    
                                del (owe[deudor][a_deb])
                                if not owe[deudor]:
                                    del owe[deudor]
                            else:
                                owe[deudor][a_deb] -= amount_can_share
                                transactions = add_transaction(a_deb, deudor, np.around(amount_can_share,decimals=3), transactions)
                                consumptions_aux[a_deb][1] -= amount_can_share
                                consumptions_aux[deudor][1] += amount_can_share
    
                                amount_can_share = 0
                            
    for elem in consumptions_aux:
        data[int(elem[0])]['neighbour'].set_consumption(time, elem[1])
    
    return transactions

def calculate_solution(neighbours_data):
    
    number_of_measurements = neighbours_data[0]['neighbour'].get_number_measurements()
    
    means = get_threshold(neighbours_data)
    
    for time in range(number_of_measurements):
        billing = generate_new_threshold(time)
        if billing:
            # TODO: Compute the new threshold if needed. For testing we manually control the billing
            # period is always within the time window.
            # means = get_threshold(neighbours_data)
            pass
        
        owe, block_number = get_all_debts(neighbours_data, 1)
        print('Debts: {}'.format(owe)) if debug else 0
        print('Round: {}'.format(time)) if debug else 0
        value = lightweight_solution_week_avg(neighbours_data, time, owe, means)
        
        print('len: {}, \t value: {}'.format(len(value), value)) if debug else 0
        
        for tran in value:
            make_transaction(host, neighbours_data, tran)
        
        mine(host)
        print() if debug else 0

    return neighbours_data


if __name__ == '__main__':
    
    buildings = getClusterBuildings()
    init = True
    contador = 1
    
    for neighbours in buildings:
        print('{}Experiment with {} neigbourhs{}'.format(Fore.BLUE, neighbours, Fore.RESET))
        total_results = []
        
        ini = 0
        filename = 'experiments/{}_meters.pkl'.format(neighbours)
        if os.path.isfile(filename):
            unpickled_df = pd.read_pickle(filename)
            ini = unpickled_df.shape[0] // neighbours
        
        for experiment in tqdm(range(ini, n_experiments)):
            threads = setThreadsNeighbours(host, neighbours)
            
            neighbours_data = initialise(neighbours, threads)
            neighbours_data = calculate_solution(neighbours_data)

            if os.path.isfile(filename):
                unpickled_df = pd.read_pickle(filename)
            else:
                unpickled_df = pd.DataFrame([])

            aux_pandas = pd.concat([aux_pandas,unpickled_df])
            aux_pandas.to_pickle(filename)

            reset()
            del (neighbours_data)

        filename = 'experiments/{}_meters.pkl'.format(neighbours)
        pandas_result = pd.read_pickle(filename)
        
        
        print("---------- RESULTS ----------")
        print("Number of experiments: {}".format(pandas_result.shape[0]//neighbours))
        print()
        
        for neigh in range(neighbours):
            to_analyze = pandas_result[pandas_result.meter == neigh]
            to_analyze['lcs_len'] = to_analyze['lcs'].str.len()
            
            print('Neighbour {}'.format(neigh))
            print('len(real peaks) {}'.format(np.mean(to_analyze['peaks_real'].apply(len).values)))
            print('len(reported peaks) {}'.format(np.mean(to_analyze['peaks_reported'].apply(len).values)))
            print('edit distance (peaks) {}'.format(np.mean(to_analyze['distance'].values)))
            print('number of common peaks (matches) {}'.format(np.mean(to_analyze['matches'].values)))
            print('longest common sequence (lcs) {}'.format(np.mean(to_analyze['lcs_len'].values)))
            print()
            
            
    
