# -*- coding: utf-8 -*-
import requests
from lxml import html
import pandas as pd
import time
import random
import logging
import sys
from timeout import timeout

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
SLEEPING_INTERVAL = 2
CSV_NAME = 'info.csv'
LOG_NAME = 'log.txt'
URL_RETRY = 2
MAIN_RETRY = 20
COUNTRY_URL_RETRY_INTERVAL = 10
CITY_URL_RETRY_INTERVAL = 10
PAGINATION_URL_RETRY_INTERVAL = 10
HANDLE_URL_RETRY_INTERVAL = 10

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)
fh = logging.FileHandler(LOG_NAME)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)


def get_tree(url):
    response = requests.get(url, headers=HEADERS)
    tree = html.fromstring(response.content)
    return tree

#@timeout(COUNTRY_URL_RETRY_INTERVAL)
def get_country_urls(base_url=DIRECTORY_URL):
    tree = get_tree(base_url)
    country_urls = tree.xpath('//div[@class="ct_city"]/a/@href')
    return country_urls

@timeout(CITY_URL_RETRY_INTERVAL)
def get_city_urls(country_url):
    print('Getting City Urls from %s'%country_url)
    city_urls = get_tree(country_url).xpath('//div[@class="ct_city"]/a/@href')
    return city_urls

@timeout(PAGINATION_URL_RETRY_INTERVAL)
def get_pagination_urls(city_url):    
    print('Getting Pagination Urls from %s'%city_url)
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

@timeout(HANDLE_URL_RETRY_INTERVAL)
def handle_url(url):
    tree = get_tree(url)
    info_block_list = get_info_blocks(tree)
    info_list = []
    for info_block in info_block_list:
        info = process_info_block(info_block) 
        info['url'] = url
        info_list.append(info)
    return info_list

class MaxRetryTimeOutException(Exception):
    pass

class RetryUrlDict():
    def __init__(self, url_list=None, retry=2):
        if url_list:
            self.retry_dict = {i:retry for i in url_list}
        else:
            self.retry_dict = {}
        self.failed_urls = []
        self.retry = retry
        
    def pop(self):
        random_url = random.choice(list(self.retry_dict.keys()))
        self.retry_dict[random_url] -= 1 
        retry_left = self.retry_dict[random_url]
        if retry_left == 0:
            self.retry_dict.pop(random_url)
        return random_url,retry_left

    def remove_succeed_url(self, url):
        self.retry_dict.pop(url)

    def add_failed_url(self, url):
        self.failed_urls.append(url)
    
    def get_failed_urls(self):
        return self.failed_urls
    
    def add_urls(self, urls):
        new_retry_dict = {i:self.retry for i in urls}
        self.retry_dict.update(new_retry_dict)

    def __bool__(self):
        return bool(self.retry_dict)
    
    
def run_func_with_timeout_retry(func, retry, interval, *args, **kwargs):
    while retry:
        try:
            return func(*args, **kwargs)
        except TimeoutError as e:
            time.sleep(interval)
            print('Sleeping %s(run_func_with_timeout_retry)'%interval)
            retry -= 1
            if retry == 0:
                raise MaxRetryTimeOutException
            continue

def main():
    '''
    2. country_url_failed.txt: 会在文件末尾append访问失败的国家url, 
    3. pagination_url_failed.txt: 会在文件末尾append访问失败的最终url
    '''
    print('Main Start')

    country_urls = run_func_with_timeout_retry(get_country_urls, retry=10, 
            interval=COUNTRY_URL_RETRY_INTERVAL)
    csv_name, failed_country_urls, failed_pagination_urls = \
    handle_country_urls(country_urls)

    for i in range(MAIN_RETRY):
        csv_name, failed_country_urls, failed_pagination_urls = \
        handle_country_urls(failed_country_urls, failed_pagination_urls)
        if not failed_country_urls and not failed_pagination_urls:
            print('All Urls are handled')
            break
        else:
            print('Remaining failed country urls:%s'%len(failed_country_urls))
            print('Remaining failed pagination urls:%s'%len(failed_pagination_urls))
            print('Retrying')
            continue

        
    print('Final Remaining failed country urls:%s'%len(failed_country_urls))
    print('Final Remaining failed pagination urls:%s'%len(failed_pagination_urls))

    print('Writing to country_url_failed.txt')
    with open('country_url_failed.txt', 'a') as f:
        f.write('\n'.join(failed_country_urls))

    print('Writing to pagination_url_failed.txt')
    with open('pagination_url_failed.txt', 'a') as f:
        f.write('\n'.join(failed_pagination_urls))
        
    print('Main End')

def handle_country_urls(country_urls, pagination_urls=None):
    '''
    Usage:
        传入country_url和pagination_url(optional), 返回抓取到的数据文件的名称,
        失败的国家url(list),以及失败的最终url(list)
    @params:
        country_urls: 国家url
        pagination_urls: 最终url
    @returns
        1. CSV_NAME: 在文件末尾append数据 
        2. failed_country_urls: 失败的国家url(list)
        3. failed_pagination_urls 失败的最终url(list)

    '''
    print('Handling country urls Start')
    n_countries = len(country_urls)
    print('Number of Countries:%s'%n_countries)

    retry_country_url_dict = RetryUrlDict(country_urls, retry=URL_RETRY)
    retry_pagination_url_dict = RetryUrlDict(retry=URL_RETRY)
    if pagination_urls:
        retry_pagination_url_dict.add_urls(pagination_urls)

    i_country = 0
    while retry_country_url_dict:
        country_url,retry_left = retry_country_url_dict.pop()
        print('Processing Country:%s/%s'%(i_country + 1, n_countries))
        try:
            city_urls = get_city_urls(country_url)
            pagination_urls_nested = [get_pagination_urls(city_url) for city_url in city_urls]
            pagination_urls = [i for lis in pagination_urls_nested for i in lis]
            print('Country successfully handled:%s'%country_url)
            if retry_left > 0:
                retry_country_url_dict.remove_succeed_url(country_url)
            retry_pagination_url_dict.add_urls(pagination_urls)
            i_country += 1
        except TimeoutError as e:
            t = round(random.random(), 2)
            print('Timeout, Sleeping %ss'%t)
            time.sleep(t)

            if retry_left <= 0:
                retry_country_url_dict.add_failed_url(country_url)
                print('Country url failed :%s'%country_url)
            continue        
    
   
        
    i_pagination = 0
    info_list_global = []
    
    n_pagination_urls = len(retry_country_url_dict.retry_dict)

    while retry_pagination_url_dict:
        pagination_url, retry_left = retry_pagination_url_dict.pop()
        print('Processing pagination url:%s/%s %s'%(i_pagination + 1, n_pagination_urls,
        pagination_url))
        try:
            info_list = handle_url(pagination_url)
            info_list_global.extend(info_list)
            if retry_left > 0:
                retry_pagination_url_dict.remove_succeed_url(pagination_url)
            i_pagination += 1
            print('Sample:',str(info_list[:1]).encode('utf8'))
            print('Pagination url successfully handled:%s'%pagination_url)
        except TimeoutError:
            t = round(random.random(),2)
            print('Timeout, Sleeping %ss'%t)
            time.sleep(t)
            if retry_left <= 0:
                retry_pagination_url_dict.add_failed_url(pagination_url)
                print('Pagination url failed :%s'%pagination_url)
            continue
        
        
        if i_pagination % 100 == 0:
            print('Sleeping %ss'%(2*SLEEPING_INTERVAL))
            time.sleep(2 * SLEEPING_INTERVAL)
            print('Saving File')
            if i_pagination == 100:
                pd.DataFrame(info_list_global).to_csv(CSV_NAME)
            else:
                with open(CSV_NAME,'a') as f:
                    pd.DataFrame(info_list_global).to_csv(f, header=False)
            print('File Saved')
            info_list_global = []
    print('Handling country urls End')
    failed_country_urls = retry_country_url_dict.get_failed_urls()
    failed_pagination_urls = retry_pagination_url_dict.get_failed_urls()
    return CSV_NAME, failed_country_urls, failed_pagination_urls

if __name__ == '__main__':
    main()