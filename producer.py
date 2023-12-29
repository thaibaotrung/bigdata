import requests
from bs4 import BeautifulSoup as bs
import csv
import time
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='masternode:9092')
start_time = time.time()

pages = []
for page_number in range(1, 20):
    url_start = 'https://www.centralcharts.com/en/price-list-ranking/'
    url_end = 'ALL/asc/ts_29-us-nyse-stocks--qc_1-alphabetical-order?p='
    url = url_start + url_end + str(page_number)
    pages.append(url)

values_list = []
for page in pages:
    webpage = requests.get(page)
    soup = bs(webpage.text, 'html.parser')

    stock_table = soup.find('table', class_='tabMini tabQuotes')
    tr_tag_list = stock_table.find_all('tr')

    for each_tr_tag in tr_tag_list[1:]:
        td_tag_list = each_tr_tag.find_all('td')

        row_values = []
        for each_td_tag in td_tag_list[0:7]:
            new_value = each_td_tag.text.strip()
            row_values.append(new_value)
        values_list.append(row_values)
        
# Gửi các bản ghi vào topic 'trung'
for row in values_list:
    message = ','.join(row).encode('utf-8')
    producer.send('trung', value=message)
# Lưu dữ liệu vào file CSV
with open('output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['Name', 'Price', 'Change', 'Open', 'High', 'Low', 'Volume'])

    for row in values_list:
        writer.writerow(row)

print('Dữ liệu đã được lưu vào file CSV.')
print('--- %s seconds ---' % (time.time() - start_time))