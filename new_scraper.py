# -*- coding: utf-8 -*-
import requests
from lxml import html
import pandas as pd
import time
import random
import logging
import sys
# import codecs
# sys.stdout = codecs.getwriter('utf8')(sys.stdout)
# sys.stderr = codecs.getwriter('utf8')(sys.stderr)

DIRECTORY_URL = 'http://www.cargoyellowpages.com/en/directory.html'
HEADERS =  {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36'
              ,'Accept': '*/*'
              ,'Accept-Encoding': 'gzip,deflate, sdch, br'
              ,'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.6,en;q=0.4,zh-TW;q=0.2'
              ,'Connection': 'keep-alive'
              ,'Cache-Control': 'no-cache'}
INTERVAL = 2
CSV_NAME = 'info.csv'
LOG_NAME = 'log.txt'

month_mapping = {'Jan'}
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)
fh = logging.FileHandler(LOG_NAME)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)


def get_tree(url):
    response = requests.get(url, headers=HEADERS)
    tree = html.fromstring(response.content)
    return tree

def get_country_urls(base_url=DIRECTORY_URL):
    tree = get_tree(base_url)
    country_urls = tree.xpath('//div[@class="ct_city"]/a/@href')
    return country_urls

def get_city_urls(country_url):
    city_urls = get_tree(country_url).xpath('//div[@class="ct_city"]/a/@href')
    return city_urls

def get_pagination_urls(city_url):    
    # url有分页，需要把所有分页都加进来
    pagination_urls = []
    city_tree = get_tree(city_url)
    pagination = city_tree.xpath('//div[@class="pagination"]')[0]
    try:
        last_page_number = pagination.xpath('span/a/@href')[-1].split('_')[1].split('.')[0]
    except IndexError as e:
        last_page_number = 1
    try:
        pagination_urls = [city_url + 'page_' + str(i).zfill(2) + '.html' for i in range(1, int(last_page_number) + 1)]
    except TypeError as e:
        print(city_url,last_page_number)
        raise
    return pagination_urls

def get_info_blocks(tree):
    info_block_list = tree.xpath('//*[@class="bubbleInfo"]')
    return info_block_list

def process_info_block(info_block):
    try:
        company_name = info_block.xpath('div/div/h2/span/a')[0].text.strip()
    except (IndexError,UnicodeDecodeError) as e:
        company_name = ''
    
    try:
        street_address = info_block.xpath('descendant::span[@class="street-address"]')[0].text.strip()   
    except (IndexError,UnicodeDecodeError) as e:
        street_address = ''
    try:
        phone_number = info_block.xpath('div/div[5]')[0].xpath('text()')[0].strip()
    except IndexError as e:
        phone_number = ''
    try:
        email = '@'.join(info_block.xpath('div/*/a[contains(@class,"mailinline")]')[0].xpath('text()')).strip()
    except IndexError as e:
        email = ''
    try:
        home_page = info_block.xpath('div/div[9]/span')[0].xpath('text()')[0].strip()
    except IndexError as e:
        home_page = ''
   
    try:
        updated = info_block.xpath('*//span[@class="updated"]')[0].text.strip()
    except IndexError as e:
        updated = ''

    return {'company_name':company_name, 'street_address':street_address,
        'phone_number':phone_number, 'email':email, 'home_page':home_page,
        'updated':updated
        }

def handle_url(url):
    tree = get_tree(url)
    info_block_list = get_info_blocks(tree)
    info_list = []
    for info_block in info_block_list:
        info = process_info_block(info_block) 
        info['url'] = url
        info_list.append(info)
    return info_list
         
def main():
    info_list_global = []
    country_urls = get_country_urls()
    n_pagination_urls_processed = 0
    n_info_processed = 0
    n_pagination_urls_skipped= 0
    n_countries = len(country_urls)
    # 读取info.csv里的url,这部分是已经抓取过的，不再重复处理
    try:
        old_df = pd.read_csv(CSV_NAME)
        pagination_urls_processed = set(old_df['url'])
    except (FileNotFoundError,KeyError) as e:
        pagination_urls_processed = set()
        old_df = pd.DataFrame()

    print('Number of Countries:%s'%n_countries)
    
    for i,country_url in enumerate(country_urls):
        city_urls = get_city_urls(country_url)
        for city_url in city_urls:
            pagination_urls = get_pagination_urls(city_url)
            for pagination_url in pagination_urls:
                if pagination_url in pagination_urls_processed:
                    n_pagination_urls_skipped += 1
                    print('Urls Skipped:',n_pagination_urls_skipped)
                    continue
                try:
                    print('Pagination url:',pagination_url)
                    info_list = handle_url(pagination_url)
                    n_pagination_urls_processed += len(pagination_urls)
                    n_info_processed += len(info_list)
                    print('Country:%s/%s'%(i+1, n_countries))
                    print('Info Processed:',n_info_processed)
                    print('Sample:',str(info_list[:1]).encode('utf8'))
                    print('Url Processed:',n_pagination_urls_processed)
                    info_list_global.extend(info_list)
                except Exception as e:
                    logger.error('info list%s'%info_list)
                    logger.error('city url:%s'%city_url)
                    logger.error('pagination url:%s'%pagination_url)
                    logger.error('pagination_urls:%s'%pagination_urls)
                    logger.error('n_info_processed:%s'%n_info_processed)
                    logger.error('n_pagination_urls_processed:%s'%n_pagination_urls_processed)
                    raise
            time.sleep(INTERVAL + random.random())
            new_df = pd.concat([old_df, pd.DataFrame(info_list_global)], axis=0)
            new_df.to_csv(CSV_NAME)       
            old_df = new_df
            info_list_global = []
        time.sleep(2 * INTERVAL)
    time.sleep(10 * INTERVAL)
    


if __name__ == '__main__':
    main()