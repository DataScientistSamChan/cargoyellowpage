# -*- coding: utf-8 -*-
import requests
from lxml import html
import pandas as pd
import time
import random
import logging
import sys
# from timeout import timeout
from requests.exceptions import ConnectTimeout, ConnectionError, ReadTimeout
from socket import timeout
from urllib3.exceptions import ReadTimeoutError
import threading
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
SLEEPING_INTERVAL = lambda:random.random()
CSV_FILE_PATH = 'info.csv'
LOG_NAME = 'log.txt'
URL_RETRY = 2
MAIN_RETRY = 20
COUNTRY_URL_RETRY_INTERVAL = 20
CITY_URL_RETRY_INTERVAL = 20
PAGINATION_URL_RETRY_INTERVAL = 20
HANDLE_URL_RETRY_INTERVAL = 10


formatter = logging.Formatter('%(asctime)s  - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler(LOG_NAME, 'a', encoding='utf-8') 
handler.setFormatter(formatter)
logger.addHandler(handler)

handler_stdout = logging.StreamHandler(sys.stdout)
handler_stdout.setLevel(logging.INFO)
handler_stdout.setFormatter(formatter)
logger.addHandler(handler_stdout)

lock = threading.Lock()

def get_tree(url, timeout=None):
    response = requests.get(url, headers=HEADERS, timeout=timeout)
    tree = html.fromstring(response.content)
    return tree

#@timeout(COUNTRY_URL_RETRY_INTERVAL)
def get_country_urls(base_url=DIRECTORY_URL,timeout=None):
    if not timeout:
        timeout = COUNTRY_URL_RETRY_INTERVAL
    tree = get_tree(base_url)
    country_urls = tree.xpath('//div[@class="ct_city"]/a/@href')
    return country_urls

# @timeout(CITY_URL_RETRY_INTERVAL)
def get_city_urls(country_url,timeout=None):
    if not timeout:
        timeout=CITY_URL_RETRY_INTERVAL
    logger.info('Getting City Urls from %s'%country_url)
    city_urls = get_tree(country_url, timeout=timeout).xpath('//div[@class="ct_city"]/a/@href')
    return city_urls

#@timeout(PAGINATION_URL_RETRY_INTERVAL)
def get_pagination_urls(city_url,timeout=None):    
    if not timeout:
        timeout = PAGINATION_URL_RETRY_INTERVAL
    logger.info('Getting Pagination Urls from %s'%city_url)
    # url有分页，需要把所有分页都加进来
    pagination_urls = []
    city_tree = get_tree(city_url,timeout=timeout)
    pagination = city_tree.xpath('//div[@class="pagination"]')[0]
    try:
        last_page_number = pagination.xpath('span/a/@href')[-1].split('_')[1].split('.')[0]
    except IndexError as e:
        last_page_number = 1
    try:
        pagination_urls = [city_url + 'page_' + str(i).zfill(2) + '.html' for i in range(1, int(last_page_number) + 1)]
    except TypeError as e:
        logger.info(city_url,last_page_number)
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

# @timeout(HANDLE_URL_RETRY_INTERVAL)
def handle_url(url, timeout=None):
    if not timeout:
        timeout = HANDLE_URL_RETRY_INTERVAL
    tree = get_tree(url, timeout=timeout)
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
    def __init__(self, process_url, logger, url_list=None, retry=2, 
                url_name=None, exceptions=None, interval=None,
                max_threads=10):
        self.failed_urls = []
        self.retry = retry
        self.process_url = process_url
        self.logger = logger
        self.url_name = url_name
        self.interval = interval
        self.process_url_results = []
        self.max_threads = max_threads
        if url_list:
            self.retry_dict = {i:retry for i in url_list}
        else:
            self.retry_dict = {}
        if exceptions is None:
            self.exceptions = (timeout,ConnectTimeout, ConnectionError,
             ReadTimeoutError, ReadTimeout)
        else:
            self.exceptions = exceptions
        self.n_urls = len(self)
        
    def pop(self):
        random_url = random.choice(list(self.retry_dict.keys()))
        self.retry_dict[random_url] -= 1 
        retry_left = self.retry_dict[random_url]
        if retry_left == 0:
            self.retry_dict.pop(random_url)
        return random_url,retry_left

    def remove_succeed_url(self, url):
        try:
            self.retry_dict.pop(url)
        except KeyError:
            pass

    def add_failed_url(self, url):
        self.failed_urls.append(url)
    
    def get_failed_urls(self):
        return self.failed_urls
    
    def add_urls(self, urls):
        new_retry_dict = {i:self.retry for i in urls}
        self.retry_dict.update(new_retry_dict)

    def __len__(self):
        return len(self.retry_dict)

    def __bool__(self):
        return bool(self.retry_dict)
    
    def process_urls(self):
        self.threaded_process_urls()

    def threaded_process_urls(self):
        threads = []

        logger.info('Processing %s Url Start'%self.url_name)
        while threads or self:
            print('Checking for Dead Threads')
            for thread in threads:
                if not thread.is_alive():
                    print('Dead Thread removed')
                    threads.remove(thread)
            
            
            while len(threads) < self.max_threads and self:
                print('Spawning new threads %s'%(len(threads) + 1))
                thread = threading.Thread(target=self.process_url_worker)
                thread.setDaemon(True) # set daemon so main thread can exit when receives ctrl-c
                thread.start()
                threads.append(thread)
            
            # all threads have been processed
            # sleep temporarily so CPU can focus execution on other threads
            # SLEEP_TIME=1
            print('Sleeping for CPU execution')
            time.sleep(1)

        logger.info('Processing %s Url End'%self.url_name)
        print('process_url_results:%s'%len(self.get_results()))

    def process_url_worker(self):        
        
        while True:
            n_urls = len(self)
            try:
                with lock:
                    url, retry_left = self.pop()
                self.logger.info('Processing %s url:%s/%s %s'%(self.url_name, n_urls,
                    self.n_urls, url))
            except IndexError:
                # 没有url了
                break
            
            try:
                ret = self.process_url(url,n_urls,self.logger, round(self.interval() ,2))
                logger.info('%s successfully processed:%s'%(self.url_name, url))
                with lock:
                    self.process_url_results.append(ret)
                    if retry_left > 0:
                        self.remove_succeed_url(url)
            except self.exceptions as e:
                logger.info('Timeout, Sleeping %ss'%round(self.interval(), 2))
                time.sleep(self.interval())
                
            except Exception as e:
                logger.error('Exception url:%s, %s'%(url,e))
                
            if retry_left <= 0:
                    self.add_failed_url(url)
                    logger.info('%s url failed: %s'%(self.url_name, url))
                
    def get_results(self):
        return self.process_url_results


def process_country_url(country_url, i_url, logger, interval):
    city_urls = get_city_urls(country_url)
    with open('city_urls.txt', 'a',encoding='utf8') as f:
        f.write('\n'.join(city_urls))
    return city_urls

def process_city_url(city_url, i_url, logger, interval):
    pagination_urls = get_pagination_urls(city_url)
    with open('pagination_urls.txt','a',encoding='utf8') as f:
        f.write('\n'.join(pagination_urls))
    return pagination_urls

def process_pagination_url(pagination_url, i_url, logger, interval):
    info_list = handle_url(pagination_url)
    logger.info('Sample:%s'%str(info_list[:1]).encode('utf8'))
    return info_list

SETUP = [
            {'process_url':process_country_url,
                'logger': logger,
                'retry':URL_RETRY,
                'url_name': 'Country',
                'interval': SLEEPING_INTERVAL},
              {'process_url':process_city_url,
                'logger': logger,
                'retry':URL_RETRY,
                'url_name': 'City',
                'interval': SLEEPING_INTERVAL},
              {'process_url':process_pagination_url,
                'logger': logger,
                'retry':URL_RETRY,
                'url_name': 'Pagination',
                'interval': SLEEPING_INTERVAL},
            ]


def run_func_with_timeout_retry(func, retry, interval, *args, **kwargs):
    while retry:
        try:
            return func(*args, **kwargs)
        except TimeoutError as e:
            time.sleep(interval)
            logger.info('Sleeping %s(run_func_with_timeout_retry)'%interval)
            retry -= 1
            if retry == 0:
                raise MaxRetryTimeOutException
            continue

def handle_country_urls(csv_file_path, retry_url_dicts):
    '''
    Usage:
        传入country_url和pagination_url(optional), 返回抓取到的数据文件的名称,
        失败的国家url(list),以及失败的最终url(list)
    @params:
        csv_file_path: csv文件路径
        country_urls: 国家url
        pagination_urls: 最终url
    @returns
        1. csv_file_path: 在文件末尾append数据 
        2. failed_country_urls: 失败的国家url(list)
        3. failed_pagination_urls 失败的最终url(list)

    '''
    logger.info('Handling country urls Start')
    n_countries = len(country_urls)
    logger.info('Number of Countries:%s'%n_countries)

    nested_failed_urls_lis = []
    for i, ru_dict in enumerate(retry_url_dicts):
        if i > 0:
            ru_dict.add_urls(flat_ret)
        ru_dict.process_urls()
        ret = ru_dict.get_results()
        failed_urls_lis = ru_dict.get_failed_urls()
        nested_failed_urls_lis.append(failed_urls_lis)
        flat_ret = [j for lis in ret for j in lis]
    
    logger.info('Saving File Final ')
    with open(csv_file_path,'a', encoding='utf8') as f:
        pd.DataFrame(flat_ret).to_csv(f, header=False, index=False, encoding='utf-8')
    logger.info('File Saved Final ')
    logger.info('Handling country urls End')

    return csv_file_path, nested_failed_urls_lis
            
def get_retry_url_dicts(nested_urls_lis, setup=SETUP):
        
    for i, url_lis in enumerate(nested_urls_lis):
        setup[i]['url_list'] = url_lis

    retry_url_dicts = []
    for dict_ in setup:
        retry_url_dict =   RetryUrlDict(**dict_
                                        )
        retry_url_dicts.append(retry_url_dict)
    return retry_url_dicts
    


def main(country_urls=None, city_urls=[], pagination_urls=[], 
        n_test_country_urls=None):
    '''
    '''
    logger.info('Main Start')

    global COUNTRY_URL_RETRY_INTERVAL,CITY_URL_RETRY_INTERVAL,\
            PAGINATION_URL_RETRY_INTERVAL,HANDLE_URL_RETRY_INTERVAL
            
    if country_urls is None:
        country_urls = run_func_with_timeout_retry(get_country_urls, retry=10, 
                interval=COUNTRY_URL_RETRY_INTERVAL)

    if n_test_country_urls:
        country_urls = country_urls[:n_test_country_urls]

    
    
    retry_url_dicts = get_retry_url_dicts([country_urls, city_urls, pagination_urls])
    
    logger.info('Creating CSV_FILE')
    csv_file_path = CSV_FILE_PATH
    # 创建并清除文件内容(if any)
    with open(csv_file_path, 'w') as f:
        pass

    logger.info('CSV_FILE Created')

    csv_file_path, nested_failed_urls_lis = \
    handle_country_urls(csv_file_path=csv_file_path, retry_url_dicts=retry_url_dicts)
    

    for i in range(MAIN_RETRY):
        if i > 0:
            COUNTRY_URL_RETRY_INTERVAL += 5
            CITY_URL_RETRY_INTERVAL += 5
            PAGINATION_URL_RETRY_INTERVAL += 5
            HANDLE_URL_RETRY_INTERVAL += 5
        csv_file_path, nested_failed_urls_lis = \
        handle_country_urls(csv_file_path, retry_url_dicts)
        if sum(len(i) for i in nested_failed_urls_lis) <= 0:
            logger.info('All Urls are handled')
            break
        else:
            for i, failed_urls_lis in enumerate(nested_failed_urls_lis):
                name = SETUP[i]['url_name']
                logger.info('Remaining failed %s urls:%s'%(name, len(failed_urls_lis)))
            logger.info('Retrying')
            retry_url_dicts = get_retry_url_dicts(nested_failed_urls_lis)
            continue
    
    for i, failed_urls in enumerate(failed_urls_lis):
        
        logger.info('Final Remaining failed %s urls:%s'%(name, len(failed_urls)))
        logger.info('Writing to level_%s_url_failed.txt'%name)
        with open('level_%s_url_failed.txt'%name, 'w') as f:
            f.write('\n'.join(failed_urls))

        
    logger.info('Main End')
if __name__ == '__main__':
    test = 1
    n_test_country_urls = 0
    if test:
        if n_test_country_urls:
            main(n_test_country_urls=n_test_country_urls)
        else:
            country_urls=['http://www.cargoyellowpages.com/en/mauritius/',
                        ]#'http://www.cargoyellowpages.com/en/kazakhstan/'
            city_urls = ['http://www.cargoyellowpages.com/en/mauritius/pointe-aux-sables/']
            pagination_urls=['http://www.cargoyellowpages.com/en/brazil/curitiba/page_02.html',
                            ]
            main(country_urls=country_urls, city_urls=city_urls, pagination_urls=pagination_urls)
    else:
        main()