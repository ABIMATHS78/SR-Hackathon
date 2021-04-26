import scrapy
# from scrapy.spiders import CrawlSpider, Rule
# from scrapy.linkextractors import LinkExtractor
# from company_scraper.items import CsimarketItem
# import pandas as pd

class CsimarketSpider(scrapy.Spider):
    name = 'csimarket'
    allowed_domains = ['csimarket.com']
    start_urls = ['https://csimarket.com/stocks/at_glance.php?code=AAPL']

    # rules = [Rule(callback='parse_item')]

    def parse_item(self, response):
        # fabric = CsimarketItem()

        def text(elt):
            return elt.xpath('./text()').extract_first(default='')


        for table in response.xpath('//table[@class="comgl"]'):
            # header = [text(th) for th in table.xpath('//th')]
            data = {}
            for tr in table.xpath('//tr'): 
                # td = tr.xpath('//td')
                # print(td[0], td[1])
                i = 1
                hold = False
                for td in tr.xpath('td'):
                    if i == 1:
                        if "Market Capitalization" in text(td):
                            hold = True
                            # data["market_capitalization"] = text(td[1])
                    if i == 2 and hold == True:
                        print(text(td))

                    i += 1
            data["market_capitalization"] = table

        return data
