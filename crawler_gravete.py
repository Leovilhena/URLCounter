# -*- coding: utf-8 -*-

import os
import re
import asyncio
import lxml.etree
import uvloop
import traceback
# import random
import aiohttp
import logging
from logging import handlers
from fake_useragent import UserAgent
from aiohttp import ClientSession

# Counts how many pages we have on a website


class Crawler:

    def __init__(self, name):
        self.__name = name
        self.logger = logging.getLogger()
        self.requests = 0
        self.ok = 0
        self.urls = set()
        self.exceptions = 0

        self.file = 'gravete_hrefs.'
        self.fcounter = 0

    def log_settings(self, level=logging.INFO, filename=None, file_size=10000000, backup=1, write_mode='a'):
        """Logging settings"""
        """Please set the log name and watch out for too heavy files!!!
        Documentation https://docs.python.org/2/howto/logging.html"""

        if not backup:
            backup = 1

        # Filename
        if not filename:
            filename = "".join([self.__name.replace('.', '_'), '.log'])

        # Create console/stream handler and file handler
        # stream_handler = logging.StreamHandler(sys.stdout)
        file_handler = logging.handlers.RotatingFileHandler(filename=filename,
                                                            mode=write_mode,
                                                            encoding='utf8',
                                                            maxBytes=file_size,
                                                            backupCount=backup)
        # Create formatter
        message_file = '[*] %(asctime)s [%(levelname)-8s] [%(processName)-10s] %(funcName)s() :\n%(message)s'
        formatter_file = logging.Formatter(message_file)

        # message_stream = '[%(levelname)-8s] %(funcName)s() : %(message)s'
        # formatter_stream = logging.Formatter(message_stream)
        # Add formatter to handler
        # stream_handler.setFormatter(formatter_stream)
        file_handler.setFormatter(formatter_file)
        # Sets different level for handlers
        # stream_handler.setLevel(logging.INFO)
        # file_handler.setLevel(logging.DEBUG)
        # Add handlers to logger
        # self.logger.addHandler(stream_handler)
        self.logger.addHandler(file_handler)
        # Set logging level
        self.logger.setLevel(level)
        self.logger.propagate = False

    @staticmethod
    def chunks_division(data_list, size=1000):
        """Returns successive n-sized chunks from l."""
        if not data_list:
            print('No data found for chunking')
            return []
        return [data_list[x:x + size] for x in range(0, len(data_list), size)]

    def crawl(self, urls):
        """Fetch list of web pages asynchronously."""
        crawled_urls = self.urls
        file_path = self.file + str(self.fcounter) + '.csv'
        crawled_urls.add(urls[0])

        requeued = {}

        while len(urls):
            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
            loop = asyncio.get_event_loop()  # event loop
            future = asyncio.ensure_future(self.fetch_all(urls))
            crawled_recently = loop.run_until_complete(future)

            del urls[:]
            for urls_list in crawled_recently:  # url is a list
                if urls_list is None or not len(urls_list):
                    continue

                # print(urls_list)

                # Requeue
                if urls_list[0] is True:
                    url = urls_list[1]
                    # print('[*] From exec {}'.format(url))
                    if 'gravete.com' not in url:
                        continue

                    if 'email-protection' not in url and 'redirect' not in url:
                        if url in requeued:  # if requeued once
                            if requeued[url] >= 10:
                                continue
                            else:
                                requeued[url] += 1
                        else:
                            requeued[url] = 0
                        urls.append(url)
                    continue

                for url in urls_list:
                    if 'gravete.com' not in url:
                        continue
                    if url not in crawled_urls:
                        crawled_urls.add(url)
                        if 'email-protection' not in url and 'redirect' not in url:
                            urls.append(url)
            try:
                file_size = os.path.getsize(file_path)/1000000
                if file_size > 10:
                    self.fcounter += 1
                    file_path = self.file + str(self.fcounter) + '.csv'
            except KeyboardInterrupt:
                exit(1)
            except:
                pass

            with open(file_path, 'a+') as file:
                for url in urls:
                    file.write(url + '\n')

            print('[*] To crawl {}'.format(len(urls)))
        print('[*] Crawling end!')
        print('[*] Sent: {} requests'.format(self.requests))
        print('[*] Crawled: {} pages'.format(self.ok))
        print('[*] Exceptions: {}'.format(self.exceptions))

        with open(file_path, 'a+') as file:
            file.write('[*] Crawled: {} pages\n'.format(self.ok))

    # def random_proxy(self):
    #     return random.choice(self.HTTPS_PROXY)

    def parse_href(self, content):
        tree = lxml.etree.HTML(content)
        xlinks = tree.xpath('//a/@href')

        if not len(xlinks):
            return

        url_joiner = lambda x: x if x.startswith('http') else (
            'https://gravete.com' + x if not x.startswith('www') else 'https://' + x)

        results = set(url_joiner(link) for link in xlinks if link != '/' and len(link) and '/tech' not in link)

        # bs = BeautifulSoup(content, "html5lib")
        # bs_results = bs.find_all('a', href=True)
        #
        # for result in bs_results:
        #     results.add(url_joiner(result))

        re_urls = re.findall(r'href=[\'"]?([^\'" >]+)', content)

        for result in re_urls:
            results.add(url_joiner(result))

        return list(results)

    async def fetch_all(self, urls, batches=100):
        """Launch requests for all web pages."""

        tasks = []
        results = None
        urls = self.chunks_division(urls, batches)
        ua = UserAgent()

        while True:
            async with ClientSession(
                    conn_timeout=30,
                    read_timeout=90,
                    raise_for_status=False,
                    headers={'user-agent': ua.random},
            ) as session:
                for batch in urls:
                    # print('[*] Crawling a batch of {} entries'.format(len(batch)))

                    for url in batch:
                        task = asyncio.ensure_future(self.get(url, session))
                        tasks.append(task)  # create list of tasks
                    results = await asyncio.gather(
                        *tasks,
                        return_exceptions=False
                    )
                    print('[*] Crawled: {} pages'.format(self.ok))
                return results

    async def get(self, url, session):
        """Fetch a url, using specified ClientSession."""
        print('[*] Fetching page: {}'.format(url))
        try:

            async with session.get(
                    url,
            ) as response:
                resp = ''
                self.requests += 1  # stats
                if response.status == 200:
                    # print('[*] OK')
                    text = await response.text(encoding='utf-8', errors='replace')

                    if not text or text is None:
                        return [True, url]

                    resp = self.parse_href(str(bytearray(text, encoding='utf-8')))
                    self.ok += 1  # stats
                return resp
        except KeyboardInterrupt:
            exit(1)
        except (
                aiohttp.client_exceptions.ClientResponseError,
                aiohttp.client_exceptions.ClientConnectionError,
                aiohttp.client_exceptions.ServerTimeoutError,
                aiohttp.client_exceptions.ServerDisconnectedError,
        ):
            self.logger.error(traceback.print_exc(limit=2))
            print('[!] Exception!')
            self.exceptions += 1
            return [True, url]
        except:
            self.logger.error(traceback.print_exc(limit=2))
            print('[!] Exception!')
            self.exceptions += 1
            return [True, url]


if __name__ == '__main__':
    URL_LIST = [
        'https://gravete.com',
    ]
    crawler = Crawler('gravete.com')
    crawler.crawl(URL_LIST)

