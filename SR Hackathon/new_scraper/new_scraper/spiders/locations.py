import scrapy
import os
import json
import pandas as pd
import csv
import re

class CsimarketSpider(scrapy.Spider):
    name = 'csimarket'

    def __init__(self, *a, **kw):
        super(CsimarketSpider, self).__init__(*a, **kw)

    def start_requests(self):
        base_url = 'https://headquartersoffice.com/'
        #company_dict = {"Apple Inc":"AAPL", "Kellogg Company":"K", "Visa Inc":"V", "Wells Fargo and Company":"WFC", "Bank Of America Corporation":"BAC", "United Airlines Holdings Inc":"UAL", "Ford Motor Co":"F", "Exxon Mobil Corporation":"XOM", "Chevron Corp":"CVX", "Johnson and Johnson":"JNJ", "Amazon Com Inc":"AMZN", "Abbvie inc":"ABBV", "Biogen Inc":"BIIB", "Fedex Corporation":"FDX", "Hershey Co":"HSY", "Tesla Inc":"TSLA"}
        spec_map = {"Apple Inc":"apple", "Visa Inc":"visa", "Wells Fargo and Company":"wells-fargo", "Abbvie inc":"abbvie", "Fedex Corporation":"fedex"}
        headers = ["Zip Code", "Head Quarters", "ISIN", "Founded", "Founder", "Name of Company"]
        headers2 = ["Name of Company", "City", "Country"]
        
        save_path = 'C:/Users/training/Documents/SR Hackathon/new_scraper/new_scraper/spiders/'

        if not os.path.exists(save_path):
            os.makedirs(save_path)

        with open(os.path.join(save_path, 'extract_others'+".csv"), 'a', newline='') as out_file_csv:
            writer = csv.writer(out_file_csv)
            writer.writerow(headers)

        with open(os.path.join(save_path, 'extract_location'+".csv"), 'a', newline='') as out_file_csv:
            writer = csv.writer(out_file_csv)
            writer.writerow(headers2)

        for name, company in spec_map.items():
            url = base_url+str(company)
            yield scrapy.Request(url=url, callback=self.parse_company, meta={'name':name, 'headers':headers, 'company':company, 'headers2':headers2})


    def parse_company(self, response):
        company = response.meta['company']
        name = response.meta['name']
        loc_map = {"CA":"California", "MI":"Michigan", "NC":"North Carolina", "IL":"Illinois", "TX":"Texas", "NJ":"New Jersey", "WA":"Washington", "MA":"Massachusetts", "TN":"Tennessee", "PA":"Pennsylvania"}
        save_path = 'C:/Users/training/Documents/SR Hackathon/new_scraper/new_scraper/spiders/'

        def text(elt):
            return elt.xpath('./text()').extract_first(default='')


        data = {}


        try:
            zip_code = response.xpath('//td[@width="140" and contains(text(), "Zip Code:")]/following-sibling::td[1]//text()').get()
            if zip_code != None:
                data['Zip Code'] = zip_code
            else:
                data['Zip Code'] =''
        except:
            data['Zip Code'] = ''

        try:
            head_q = response.xpath('//td[@width="140" and text()="HQ:"]/following-sibling::td[1]//text()').get()
            if head_q != None:
                data['Head Quarters'] = head_q
            else:
                data['Head Quarters'] =''
        except:
            data['Head Quarters'] = ''

        try:
            isin = response.xpath('//td[contains(text(), "ISIN")]/following-sibling::td[1]/a//text()').get()
            if isin != None:
                data['ISIN'] = isin
            else:
                data['ISIN'] =''
        except:
            data['ISIN'] = ''

        try:
            founded = response.xpath('//td[contains(text(), "Founded")]/following-sibling::td[1]//text()').get()
            try:
                founded = re.search(r'\d{4}', founded).group().rstrip()
            except:
                pass
            if founded != None:
                data['Founded'] = founded
            else:
                data['Founded'] =''
        except:
            data['Founded'] = ''

        try:
            founder = response.xpath('//td[contains(text(), "Founder")]/following-sibling::td[1]//text()').get()
            if founder != None:
                data['Founder'] = founder
            else:
                data['Founder'] =''
        except:
            data['Founder'] = ''


        try:
            if name != None:
                data['Name of Company'] = name
            else:
                data['Name of Company'] =''
        except:
            data['Name of Company'] = ''


        data2 = {}
        # data['Head Quarters'] = response.xpath('//td[@width="140" and text()="HQ:"]/following-sibling::td[1]//text()').get()
        # data['ISIN'] = response.xpath('//td[contains(text(), "ISIN")]/following-sibling::td[1]/a//text()').get()
        # data['Founded'] = response.xpath('//td[contains(text(), "Founded")]/following-sibling::td[1]//text()').get()
        # data['Founder'] = response.xpath('//td[contains(text(), "Founder")]/following-sibling::td[1]//text()').get()
        ## following::tr/td/a/following::a[1]/following::tr/td//text()
        location_list = list(response.xpath('//td[@style="text-align: center;" and @colspan=2]/following::td[3]//text()').getall())
        location_list2 = []
        for loc in location_list:
            try:
                loc_ = loc.split(',')
                if len(loc_) >= 2:
                    city = loc_[-2].rstrip().lstrip()
                    country_ = loc_[-1].rstrip().lstrip()
                    city2 = ""
                    for word in city.split():
                        if "+" not in word:
                            city2 += (word+" ")
                    country2 = ""
                    for word in country_.split():
                        if "+" not in word:
                            country2 += (word+" ")
                else:
                    city2 = ""
                    country_ = loc.split(',')[-1].rstrip().lstrip()
                    country2 = ""
                    for word in country_.split():
                        if "+" not in word:
                            country2 += (word+" ")
            except:
                city2 = ""
                country2 = ""
            location_list2 += [[city2.rstrip().lstrip(), country2.rstrip().lstrip()]]

        # data['Locations'] = location_list2

        print(data)
        print(location_list2)
        # print(list(response.xpath('//td[@style="text-align: center;" and @colspan=2]/following::td[3]//text()').getall()))

        if not os.path.exists(save_path):
            os.makedirs(save_path)
        with open(os.path.join(save_path, 'extract_others'+".json"), 'a') as out_file_json:
            out_json = json.dumps(data)
            out_file_json.write(out_json+"\n")

        with open(os.path.join(save_path, 'extract_others'+".csv"), 'a', newline='') as out_file_csv:
            writer = csv.writer(out_file_csv)
            writer.writerow(list(data.values()))


        # with open(os.path.join(save_path, 'extract_location'+".json"), 'a') as out_file_json:
        #     out_json = json.dumps(data2)
        #     out_file_json.write(out_json+"\n")

        with open(os.path.join(save_path, 'extract_location'+".csv"), 'a', newline='') as out_file_csv:
            for pts in location_list2:
                writer = csv.writer(out_file_csv)
                writer.writerow([name]+pts)

