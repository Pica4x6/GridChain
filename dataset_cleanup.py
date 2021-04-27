import glob, os
import pandas as pd
from tqdm import tqdm
import numpy as np

pd.set_option('mode.chained_assignment', None)

def fillDataset(df, insertions, indexes):
    for cont in indexes:
        if cont!=0:
            difference = df.loc[df.index[cont]]['DateTime'] - df.loc[df.index[cont - 1]]['DateTime']
            if difference > pd.offsets.Minute(30):
                ds = df.loc[df.index[cont]]
                ds['DateTime'] = ds['DateTime'] - pd.offsets.Minute(30)
                ds['Consumption'] = np.average([ds['Consumption'], df.loc[df.index[cont - 1]]['Consumption']])
                df = df.append(ds)
                df = df.sort_values('DateTime').reset_index(drop=True)
                insertions += 1
            elif difference < pd.offsets.Minute(30):
                print('Less than 30')
    
    return df, insertions

def printFile(filename, cad):
    with open(filename, 'a') as f:
        f.write(cad)

def exist(filename):
    if os.path.isfile("datasets/cleaned/{}".format(filename.split('/')[-1])):
        return True
    else:
        return False

if __name__ == '__main__':
    
    files = glob.glob('datasets{}london_dataset{}*.csv'.format(os.sep, os.sep))
    files.sort()
    
    for file in tqdm(files):
        
        if not exist(file):
            
            output = pd.DataFrame({})
            
            data = pd.read_csv(file, na_values=['Null', '.'])
            data.rename(columns={'KWH/hh (per half hour) ': 'Consumption'}, inplace=True)
            data['DateTime'] = pd.to_datetime(data["DateTime"])

            # 1) delete nan values
            data = data.dropna()

            # 2) Delete duplicated hours
            data = data.drop_duplicates(['LCLid', 'DateTime'], keep='last')
            data = data.sort_values('DateTime')
            data = data.reset_index(drop=True)
            
            houses = list(set(data['LCLid']))
            houses.reverse()

            insertions_file = 0
            
            for house in houses:
                df = data[data['LCLid']==house]
                df = df.reset_index(drop=True)
                
                # 3) Caclulate jumps and insert missing values
                aux = df.loc[df.index[-1]]['DateTime'] - df.loc[df.index[0]]['DateTime']
                # print()
                # print('There are: {}'.format(df.shape[0]))
                # print('Time: {}'.format(aux))
                insertions_house = 0
                finish = list(df[(df['DateTime'] - df['DateTime'].shift()) != pd.offsets.Minute(30)].index)
                finish.reverse()
                
                while len(finish)>1:
                    
                    df, insertions_house = fillDataset(df, insertions_house, finish)
                    finish = list(df[(df['DateTime'] - df['DateTime'].shift()) != pd.offsets.Minute(30)].index)
                    finish.reverse()
                    
                insertions_file += insertions_house
    
                hours = (aux.components.hours * 2) + 1
                days = aux.components.days * 48
                if aux.components.minutes == 0:
                    minutes = 0
                else:
                    minutes = 1
                total = hours + days + minutes
                
                if total != df.shape[0]:
                    print('Error in {}. File: {}'.format(house, file))
                else:
                    output = pd.concat([output,df])

                cad = '----------- {} -----------\n'.format(file.split('/')[-1])
                cad += 'Initially : {}\n'.format(df.shape[0] - insertions_house)
                cad += 'Insertions : {}\n'.format(insertions_house)
                cad += 'Percentage : {}%\n\n'.format((insertions_house*100)/df.shape[0])
                printFile('datasets/logs/modifications_house.txt', cad)
                
                # print('There have been {} insertions'.format(insertions))
                # print('There are: {}'.format(df.shape[0]))
                # print('Total: {}'.format(total))
                #
                # print()
            
            output.to_csv("datasets/cleaned/{}".format(file.split('/')[-1]))
            
            cad = '----------- {} -----------\n'.format(file.split('/')[-1])
            cad += 'Initially : {}\n'.format(output.shape[0]-insertions_file)
            cad += 'Insertions : {}\n'.format(insertions_file)
            cad += 'Percentage : {}%\n\n'.format((insertions_file * 100) / output.shape[0])
            printFile('datasets/logs/modifications_file.txt',cad)