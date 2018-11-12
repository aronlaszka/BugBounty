import csv
import datetime

with open('TweetFrequency.csv', encoding='utf-8-sig') as file:
    reader = csv.reader(file)
    for row in reader:
        date = datetime.datetime.fromtimestamp(int(row[0])/1000).strftime('%Y-%m-%d')
        print(f'{date},{row[1]}')
