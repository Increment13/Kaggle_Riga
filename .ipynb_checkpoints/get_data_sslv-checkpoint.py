import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
import numpy as np
import time
from tqdm.notebook import tqdm

arr_facilities = ['Лоджия, Парковочное место', 'Парковочное место', '', 'Балкон',
                   'Лоджия', 'Балкон, Лоджия, Парковочное место',
                   'Сауна, Парковочное место', 'Балкон, Лоджия', 
                   'Балкон, Парковочное место', 'Терраса',
                   'Терраса, Парковочное место', 
                   'Балкон, Терраса', 'Балкон, Терраса, Парковочное место',
                   'Балкон, Лоджия, Терраса, Парковочное место', '01000300083001',
                    'Лоджия, Сауна',
                   'Балкон, Лоджия, Терраса', 
                   'Лоджия, Терраса, Парковочное место', 'Лоджия, Терраса', 
                   'Терраса, Сауна']

def get_link(link, time_sleep, page_num):
    time.sleep(time_sleep)
    link = link + '/page' + str(page_num) + '.html'
    r = requests.get(link)
    
    if r.status_code!=200:
        return 
    
    soup = bs(r.text, "html.parser")
    parsed_data = soup.find_all('a', class_='am')
    
    pars_links = []
    
    for data in parsed_data:
        pars_links.append(data.get('href'))
        
    return pars_links


def get_data_link(url, time_sleep):
    page_array = []
    time.sleep(time_sleep)

    link = "https://www.ss.lv"
    link += url
    
    r = requests.get(link)
    if r.status_code!=200:
        return 
    
    soup = bs(r.text, "html.parser")
        
    # данные 
    parsed_data = soup.find_all('td', class_='ads_opt')   
    # координаты
    parsed_map = soup.find_all('a', class_='ads_opt_link_map')   
    # цена
    parsed_price = soup.find_all('td', class_='ads_price')    
    # описание 
    parsed_text = soup.find_all('div', id='msg_div_msg') 
    
    for data in parsed_data:
        page_array.append(data.get_text("|"))

    if len(parsed_map)==1:
        page_array.append(parsed_map[0]['onclick'])
    else:
        page_array.append('')
    
    page_array.append(parsed_price[0].get_text())       
    page_array.append(parsed_text[0].get_text(" | "))
    
    return page_array

def get_cord(row):
    # ищем стартовую точку 
    point_start = row['map'].find('c=') + 2
    
    first_coma = row['map'][point_start:].find(',') + 1
    second_coma = row['map'][point_start+first_coma:].find(',')
    
    cord = row['map'][point_start:point_start+first_coma+second_coma]
    
    return cord    

def get_df_from_req(URL_TEMPLATE, max_pages):
    link_array = []
    print('get pages link')
    for i in tqdm(range(1,max_pages+1)):
        link_array = link_array + get_link(URL_TEMPLATE, 1, i)

    data_array = []
    print('get data from pages')
    for links in tqdm(link_array):
        data_array.append(get_data_link(links, 0.25))
    
    data_array_upd = []
    for i in data_array:
        if len(i)==11:
            i.insert(8, '')
            i.insert(8, '')
        if len(i)==12:
            i.insert(8, '')
        data_array_upd.append(i)
    
    df = pd.DataFrame(data_array, columns=['city', 'district','street','rooms','area','floor','seria','house_type','kadastr_numb','facilities', 'map','price','all_data'])
    return df 

def update_street(df):
    df[['data_street', 'map_link']] = df['street'].str.split(pat='|', n=1 , expand=True )
    df = df.drop(['city','map_link','street','kadastr_numb'], axis=1)
    return df 

def update_floor(df):
    df[['cur_floor', 'max_floor']] = df['floor'].str.split(pat='/', n=1 , expand=True )
    df = df.drop(['floor'], axis=1)
    df[['total_floor', 'lift']] = df['max_floor'].str.split(pat='/', n=1 , expand=True )
    df['lift'] = np.where(df['lift']=='лифт', 1, 0)
    df = df.drop(['max_floor'], axis=1)
    return df

def update_map_cord(df):
    df['cord_map'] = df.apply(get_cord, axis=1)

    df[['len', 'lon']] = df['cord_map'].str.split(pat=',', n=1 , expand=True )
    df = df.drop(['cord_map'], axis=1)
    df = df.drop(['map'], axis=1)

    df['lon'] = df['lon'].fillna('-1')
    df['len'] = df['len'].fillna('-1')
    df.loc[df['len']=='', 'len'] = '-1'
    df.loc[df['lon']=='', 'lon'] = '-1'

    df['len'] = df['len'].apply(lambda x: x.replace(' ',''))
    df['lon'] = df['lon'].apply(lambda x: x.replace(' ',''))
    
    return df 

def update_area(df):
    df['area'] = df['area'].apply(lambda x: x.replace(' м²',''))
    return df

def update_price(df):
    df[['price_eur', 'else_price']] = df['price'].str.split(pat='€/', n=1 , expand=True )
    df[['period', 'else_del']] = df['else_price'].str.split(pat=' ', n=1 , expand=True )
    df['price_eur'] = df['price_eur'].apply(lambda x: x.replace(' ',''))

    df['period'] = np.where(df['period']=='мес.', 30, 
                            np.where(df['period']=='день', 1, 7))

    df = df.drop(['price','else_del','else_price'], axis=1)
    return df

def update_facilities(df):
    df['facilities'] = np.where(df['facilities'].isin(arr_facilities),df['facilities'],'')
    return df

def update_rooms(df):
    df['rooms'] = np.where(df['rooms']=='-', 0, df['rooms'])
    return df
    
def update_wifi(df):
    df['all_data'] = df['all_data'].str.lower()
    df['wifi'] = np.where(df['all_data'].str.find('wifi')>0, 1,
                 np.where(df['all_data'].str.find('wi-fi')>0, 1, 0))
    return df

def update_data_types(df):
    df = df[['district','data_street','rooms','area','price_eur', 'period', 'cur_floor','total_floor', 'lift', 'seria','house_type','facilities','len','lon','wifi']]
    df['rooms'] = df['rooms'].astype('int64')
    df['area'] = df['area'].astype('float64')
    df['price_eur'] = df['price_eur'].astype('int64')
    df['cur_floor'] = df['cur_floor'].astype('int64')
    df['total_floor'] = df['total_floor'].astype('int64')
    df['len'] = df['len'].astype('float64')
    df['lon'] = df['lon'].astype('float64')

def out_df(df, direct):
    df.to_csv(direct+data_from_sslv.csv')