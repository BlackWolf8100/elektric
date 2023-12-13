import requests
from my_base import My_base
from datetime import date, timedelta, datetime
import time

KEY = '994214471'
DEVID = '1862272325'
BASE_API = 'https://dash.smart-maic.com/api'

def loader(url):
    headers = {'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0Safari/537.36'}

    responce = requests.get(url, headers = headers)
    if responce.status_code != 200:
        return False

    try:
        answ = responce.json()
    except Exception as eror:
        return False
    return answ

def main():

    db = My_base()
    db.open()
    
    today = datetime.today()
    date0 = datetime(2023, 11, 23)
    
    sql = 'DELETE FROM electro WHERE `time` >= CURDATE()'
    db.execute(sql)
    
    sql = 'SELECT MAX(`TIME`) FROM electro'
    result = db.get_one_table(sql)
    print(result) 
    if result[0]:
        lastdate = result[0].date()
        print(lastdate)
        sql = f'DELETE FROM electro WHERE `time` >= "{lastdate:%Y-%m-%d}"'
        db.execute(sql)
        dt = datetime.combine(lastdate, datetime.min.time())
    else:
        dt = date0
    
   
    while dt < today:
        print(dt)
        date1 = int(dt.timestamp())
        print(date1)

        tomorrow = dt + timedelta(days=1)
        date2 = int(datetime.combine(tomorrow, datetime.min.time()).timestamp()) - 1
        print(date2)

        retry = 3

        while(retry):
            url = f'{BASE_API}?devid={DEVID}&date1={date1}&date2={date2}&period=minute&apikey={KEY}'
            result = loader(url)

            if result:
                export_data(result, db, 'electro', date2)
                print(len(result))
                break
            else:
                print("Помилка завантаження даних з API", retry)
                time.sleep(10)
                retry -= 1
        if not retry: exit()
        dt += timedelta(days=1)
        
    db.close()
    
def convert_unix_to_datetime(unix_time):
    return datetime.fromtimestamp(unix_time).strftime('%Y-%m-%d %H:%M:%S')

def export_data(data, db, table_name, date2):
    data_out = []
    for product in data:
        for k, v in product.items():
            if isinstance(v, dict) or isinstance(v, list):
                product[k] = str(v)

            if k == 'TIME' and isinstance(v, int):
                if v > date2:
                    is_date_ok = False
                    break
                else:
                    is_date_ok = True
                product[k] = convert_unix_to_datetime(v)
        if is_date_ok:
            data_out.append([product['ID'], product['TIME'], product['STAT'], product['V'], product['A'], product['W'], product['rW'], product['Wh'], product['rWh'], product['PF'], product['T']])

    sql = f'INSERT INTO {table_name} VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
    db.executemany(sql, data_out)
    
    
if __name__ == '__main__':
    main()