# -*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup
import postgresql

######################################################################################################
#
#   Скрипт поделен несколько частей. Первая - сбор данных. Я делал разный сбор данных. Страницую kia
#   я парсил. А вот на сайт huyndai - отправлял get запрос. Дальше я правил адреса huyndai. Потому что
#   поле city_id давало лишь id города, а на саму базу было никак не выйти. Я брал город из адресов.
#   Но встречались совершенно разные примеры заполнения поля address. Поэтому я сделал большую функцию-
#   заплатку для того, чтобы получить корректные города для моего поля city. Потом я заполял таблицу.
#   Здесь я поздно понял, что мне нужно 2 таблицы. Таблица дилеров и таблица городов, которые будут
#   связаны. Таблицу городов я создал позже, а но из-за странной миграции в ruby rails я так и не смог
#   сделать между ними прямую связь. Дальше есть функция реализующая анализ. Она выводит список всех
#   городов и количества дилеров kia/huyndai в них. Для корректной проверки работы скрипта - должен
#   psql с созданной бд - omd, а так же нужно поменять psql пользователя и пароль в функции db_fill()
#   и db_fill_cities()
#
#######################################################################################################



#Передаем в header корректный UserAgent(имитируем запрос от браузера)
s = requests.Session() 
s.headers.update({
        'Referer': 'http://www.kinopoisk.ru',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; rv:45.0) Gecko/20100101 Firefox/45.0'
    })

urls ={'kia':'https://www.kia.ru/dealers/', 'huyndai':'http://www.hyundai.ru/request/getalldealers'}

def getting_html(session, url):
    'Получаем html по заданному url'
    request = session.get(url)
    return request.text

def getting_json(session, url):
    'Получаем json по заданному url'
    request = session.get(url)
    return request.json()

def kia():
    'Получаем всю информацию о дилерах kia'
    soup = BeautifulSoup(getting_html(s, urls['kia']),'lxml')
    all_cities = soup.find_all('div', {'class':'toggable'})
    database = []
    unic_city = set()
    for city in all_cities:
        all_companies = city.find_all('div', {'class':'one-dealer show_dealer'})
        for company in all_companies:
            city_name = city.find('div', {'class':'city'}).text #город
            if city_name == 'Москва и МО': city_name = 'Москва'
            unic_city.add(city_name)
            dealer_name = company.find('a', {'class':'dealer-name'}).text #название
            adress = company.find('div', {'class':'adress'}).text #адрес
            phone = company.find('div', {'class':'phone'}).find('a').text #телефон
            site = company.find('a', {'class':'gatrack'}).get('href') #сайт
            database.append({'city':city_name,'name':dealer_name,'phone':phone,'address':adress,'site':site,'model':'kia'})
    return(database, unic_city) #возвращаем базу дилеров kia + все уникальные города, где есть дилеры kia


def huyndai_city(city_id, dealer):
    'Функция-заплатка. Правим имена город для дилеров huyndai'
    if city_id.get(dealer['city_id']) == None:
        full_address = dealer['address']
        split_address = full_address.split(',')
        if full_address[:2] == 'г.' or full_address[:2] == 'Г.':
            if full_address[2] == ' ':
                if 'Альметьевск' in full_address:
                    city_id[dealer['city_id']] = 'Альметьевск'
                else:
                    city_id[dealer['city_id']] = split_address[0][3:]
            else:
                city_id[dealer['city_id']] = split_address[0][2:]
        elif 'обл.' in split_address[0]:
            city_id[dealer['city_id']] = split_address[0]
        elif 'Республика' in split_address[0] or 'область' in split_address[0]:
            city_id[dealer['city_id']] = split_address[1][4:]
        elif split_address[0] == '398059':
            city_id[dealer['city_id']] = split_address[1][4:]
        else:
            city_id[dealer['city_id']] = split_address[0]
        if dealer['city_id'] == '17':
            city_id[dealer['city_id']] = 'Санкт-Петербург'
    return city_id

def huyndai():
    'Получаем всю информацию о дилерах huyndai'
    all_dealers = getting_json(s, urls['huyndai'])
    database = []
    unic_city = set()
    city_id = {}
    for dealer in all_dealers:
        city_id = huyndai_city(city_id, dealer)
        unic_city.add(city_id[dealer['city_id']])
        name = dealer['name']
        if name == '<span>Hyundai City Store</span> АВИЛОН':
            name = 'Hyundai City Store АВИЛОН'
        phone = dealer['phone']
        site = dealer['site']
        city = city_id[dealer['city_id']]
        address = dealer['address']
        database.append({'name':name,'phone':phone,'site':site,'model':'huyndai','city':city,'address':address})
    return(database, unic_city) #возвращаем базу дилеров huyndai + все уникальные города, где есть дилеры huyndai

def db_fill(all_data):
    'Создаем таблицу в базе данных omd. Столбцы: id, имя, модель, город, адрес, сайт и телефон. Заполняем таблицу'
    db = postgresql.open('pq://postgres:1234@localhost/omd') #Логинимся в базу данных omd
    # Функция создания таблицы закомментирована по причине того, что при создании модели в Ruby Rails
    # таблица создается автоматически
    # db.execute("""
        #     CREATE TABLE cars (
        #         id SERIAL PRIMARY KEY,
        #         name text,
        #         model text,
        #         city text,
        #         address text,
        #         site text,
        #         phone text
        #     );
        # """)
    # Подготоваливаем данные к записи в таблицу dealers
    ins = db.prepare('INSERT INTO dealers (name, model, city, address, site, phone) VALUES ($1, $2, $3, $4, $5, $6)')
    for company in all_data:
        ins(company['name'],company['model'],company['city'],company['address'],company['site'],company['phone'])   
    
def analysis(all_data, kia, huyndai):
    'Исследуем количество дилеров в городах'
    data_analysis = {}
    for city in kia | huyndai:
        data_analysis[city] = {'kia': 0, 'huyndai':0}
    for dealer in all_data:
        data_analysis[dealer['city']][dealer['model']] += 1
    for j,i in data_analysis.items():
       print(i, j)


def db_fill_cities(all_data, kia_cities, huyndai_cities):
    'Cоздаем таблицу городов'
    db = postgresql.open('pq://postgres:1234@localhost/omd')
    ins = db.prepare('INSERT INTO cities (name) VALUES ($1)')
    cities = kia_cities | huyndai_cities
    for city in cities:
        ins(city)



if __name__ == '__main__':
    kia_data, kia_cities = kia() #Собрали всю информацию о дилерах kia
    huyndai_data, huyndai_cities = huyndai() #Собрали всю информацию о дилерах huyndai
    all_data = kia_data
    all_data.extend(huyndai_data) 
    db_fill(all_data) #заполняем базу данных диллеров 
    analysis(all_data, kia_cities, huyndai_cities) # анализ
    db_fill_cities(all_data, kia_cities, huyndai_cities) # заполняем базу данных городов


    
    
    

