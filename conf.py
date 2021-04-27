from datetime import datetime

##### Global Variables ####
host = '127.0.0.1'
start_date = datetime.strptime('2012-08-1 00:00', '%Y-%m-%d %H:%M') # When to start simulating
end_date = datetime.strptime('2012-08-2 00:00', '%Y-%m-%d %H:%M') # When to start simulating
end_month = datetime.strptime('1 23:30', "%d %H:%M") # day and hour to resume up all the debts
debug = False
PULL_PORTS = list(range(1,2**15))
n_experiments = 1