import pandas as pd
import numpy as np
import copy, glob, os
import matplotlib.style as style
import matplotlib.pyplot as plt
from matplotlib import rc
from datetime import datetime

rc('text', usetex=True)
pd.options.mode.chained_assignment = None
style.use('seaborn-pastel')

class London:
    counter = []
    files = glob.glob('datasets{}cleaned{}*.csv'.format(os.sep, os.sep))
    files.sort()
    
    def __init__(self, start_date=None, end_date=None):
        
        self.name = None
        self.data_ori = None
        self.set_time_range(start_date, end_date)

        values = (self.end_date - self.start_date).days * 48
        self.metadate = (self.end_date - self.start_date).days

        if not hasattr(London, 'dataSet'):
            self.__loadNewFile()
        
        self.__new_neighbour(values)
        
        self.data['block_consumption'] = copy.deepcopy(self.data['Consumption'])
    
    def reset(self):
        London.files = glob.glob('datasets{}cleaned{}*.csv'.format(os.sep, os.sep))
        London.files.sort()
        delattr(London, 'dataSet')
    
    def __loadNewFile(self):
        London.filename = London.files.pop(0)
        London.counter = []
        London.dataSet = pd.read_csv(London.filename, na_values=['Null', '.'])
        London.dataSet.rename(columns={'KWH/hh (per half hour) ': 'Consumption'}, inplace=True)
    
    def __new_neighbour(self, values):
        empty = True
        while empty:
            try:
                
                self.name = self.dataSet['LCLid'].unique()[len(self.counter)]
                aux = len(self.counter)
                London.counter.append(self.dataSet['LCLid'].unique()[aux])
            
            
                mask = (self.dataSet['LCLid'] == self.name)
                self.data_ori = self.dataSet.loc[mask]
                self.data_ori["DateTime"] = pd.to_datetime(self.data_ori["DateTime"])
                self.mask = (self.data_ori['DateTime'] >= self.start_date) & \
                            (self.data_ori['DateTime'] < self.end_date)
                self.data = copy.deepcopy(self.data_ori).loc[self.mask].drop_duplicates()
        
                empty = self.data.empty

                if self.data.shape[0] != values:
                    empty = True
            
            except:
                self.__loadNewFile()
            
        self.data_ori['Consumption'] = pd.to_numeric(self.data_ori['Consumption'])
    
    def set_mask(self, mask):
        self.mask = mask
        
    def set_time_range(self, start_date, end_date):
        if start_date and end_date:
            self.start_date = start_date
            self.end_date = end_date
        else:
            self.start_date = datetime.strptime('2013-10-12 00:00', '%Y-%m-%d %H:%M')
            self.end_date = datetime.strptime('2013-10-14 00:00', '%Y-%m-%d %H:%M')
    
    def get_number_measurements(self):
        return len(self.data['Consumption'])
    
    def plot(self):
        data = self.data.loc[self.mask]
        pie = data.plot(x='DateTime', y=['Consumption'], yticks=np.linspace(0, 2, 11))
        fig = pie.get_figure()
        fig.show()
        
    def plot_solution(self, peaks_ori=np.array([]), peaks_bloc = np.array([]), save=False, extension='.eps', prefix=''):
        pie = self.data.plot(x='DateTime', y=['Consumption', 'block_consumption'], label=[r'$\mathrm{kWh}_{\mathrm{real}}$', r'$\mathrm{kWh}_{\mathrm{reported}}$'])#, yticks=np.linspace(0,2,11))
        pie.set_ylabel(r"$\mathrm{kWh}$")
        pie.set_xlabel("Time")
        
        x = pie.lines[0].get_xdata()  # Get the x data of the distribution
        
        if peaks_ori.size:
            y = pie.lines[0].get_ydata()
            plt.plot(x[peaks_ori], y[peaks_ori], "bo")
        if peaks_bloc.size:
            y = pie.lines[1].get_ydata()
            plt.plot(x[peaks_bloc], y[peaks_bloc], "gx")
        
        
    
        
        fig = pie.get_figure()
        plt.tight_layout()
        if save:
            fig.savefig("plots/" + str(self.metadate) +"_days_"+ prefix + self.name + extension, bbox_inches='tight')
        else:
            fig.show()
    
    def get_real_consumption(self, index):
        return self.data['Consumption'].values[index]
    
    def get_real_consumptions(self):
        return self.data['Consumption'].values

    def get_consumption(self, index):
        return np.around(self.data['block_consumption'].values[index],decimals=3)
    
    def get_consumption_range(self, ini, end):
    
        mask = (self.data_ori['DateTime'] > ini) & (self.data_ori['DateTime'] <= end)
        test_data = copy.deepcopy(self.data_ori).loc[mask]

        median = np.around(np.median(test_data['Consumption'].values),decimals=3)
        return median

    def get_consumptions(self):
        return self.data['block_consumption'].values
    
    def set_consumption(self, index, new_consumption):
        self.data['block_consumption'].values[index] = new_consumption

    def get_consumption_avg_neighbour(self):
        pass
    
if __name__ == '__main__':
    aux = London()
    print (aux.name)
    aux.plot()

    print()

    aux2 = London()
    print(aux2.name)

    print()

    aux3 = London()
    print(aux3.name)

    