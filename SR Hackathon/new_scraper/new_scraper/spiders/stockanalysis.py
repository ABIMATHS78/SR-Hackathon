import scrapy
import os
import json
import pandas as pd
import csv

class StockanalysisSpider(scrapy.Spider):
    name = 'stockanalysis'

    def __init__(self, *a, **kw):
        super(StockanalysisSpider, self).__init__(*a, **kw)

    def start_requests(self):
        base_url = 'https://stockanalysis.com/stocks/'
        company_dict = {"Apple Inc":"AAPL", "Kellogg Company":"K", "Visa Inc":"V", "Wells Fargo and Company":"WFC", "Bank Of America Corporation":"BAC", "United Airlines Holdings Inc":"UAL", "Ford Motor Co":"F", "Exxon Mobil Corporation":"XOM", "Chevron Corp":"CVX", "Johnson and Johnson":"JNJ", "Amazon Com Inc":"AMZN", "Abbvie inc":"ABBV", "Biogen Inc":"BIIB", "Fedex Corporation":"FDX", "Hershey Co":"HSY", "Tesla Inc":"TSLA"}

        # headers = ["Market Capitalization", "Shares Outstanding", "Number of Employees", "Revenues", "Net Income", "Cash Flow", "Capital Exp.", "Name of Company", "Location", "Sector", "Sub-Sector", "30 Day Performance", "52 Week Average", "Operating Expenses", "Ticker Symbol", "Common Stock Value"]
        headers = ['Name of Company', 'Shares Outstanding', 'Revenues', 'Net Income', 'Operating expenses', 'Founded', 'Industry', 'Sector', 'Number of Employees', 'CEO', 'Ticker Symbol', 'ISIN', 'Capital expenditure', 'Market Capitization', 'Long term Debt', 'Common Stock Value']
        save_path = 'C:/Users/training/Documents/SR Hackathon/new_scraper/new_scraper/spiders/'

        if not os.path.exists(save_path):
            os.makedirs(save_path)

        with open(os.path.join(save_path, 'extract2'+".csv"), 'a', newline='') as out_file_csv:
            writer = csv.writer(out_file_csv)
            writer.writerow(headers)

        for name, company in company_dict.items():
            url = base_url+str(company)+"/financials"
            url2 = base_url+str(company)+"/company"
            url3 = base_url+str(company)+"/financials/cash-flow-statement"
            url4 = base_url+str(company)+"/financials/ratios"
            url5 = base_url+str(company)+"/financials/balance-sheet"
            yield scrapy.Request(url=url, callback=self.parse_company, meta={'name':name, 'headers':headers, 'url2':url2, 'url3':url3, 'url4':url4, 'url5':url5, 'company':company})


    def parse_company(self, response):
        url2 = response.meta['url2']
        url3 = response.meta['url3']
        url4 = response.meta['url4']
        url5 = response.meta['url5']

        company = response.meta['company']
        loc_map = {"CA":"California", "MI":"Michigan", "NC":"North Carolina", "IL":"Illinois", "TX":"Texas", "NJ":"New Jersey", "WA":"Washington", "MA":"Massachusetts", "TN":"Tennessee", "PA":"Pennsylvania"}

        def text(elt):
            return elt.xpath('./text()').extract_first(default='')

        data ={}

        
        data["Name of Company"] = response.meta['name']

        headers = response.meta['headers']
        # grand [data]
        # df = pd.DataFrame(data)
        # df = df.transpose()
        save_path = 'C:/Users/training/Documents/SR Hackathon/new_scraper/new_scraper/spiders/'

        try:
            share_out = response.xpath('//span[@class="ttt" and contains(text(), "Shares Outstanding") and contains(text(), "Basic")]/following::td[2]//text()').get()
            if share_out != None:
                data['Shares Outstanding'] = share_out
            else:
                data['Shares Outstanding'] =''
        except:
            data['Shares Outstanding'] = ''


        try:
            rev = response.xpath('//span[@class="ttt" and text()="Revenue"]/following::td[2]//text()').get()
            if rev != None:
                data['Revenues'] = rev
            else:
                data['Revenues'] =''
        except:
            data['Revenues'] = ''


        try:
            netinc = response.xpath('//span[@class="ttt" and text()="Net Income"]/following::td[2]//text()').get()
            if netinc != None:
                data['Net Income'] = netinc
            else:
                data['Net Income'] =''
        except:
            data['Net Income'] = ''


        try:
            opexp = response.xpath('//span[@class="ttt" and text()="Operating Expenses"]/following::td[2]//text()').get()
            if opexp != None:
                data['Operating expenses'] = opexp
            else:
                data['Operating expenses'] =''
        except:
            data['Operating expenses'] = ''


        
        yield scrapy.Request(url=url2, callback=self.parse_company2, meta={'headers':headers, 'data':data, 'url2':url2, 'url3':url3, 'url4':url4, 'url5':url5, 'company':company})



    def parse_company2(self, response):

        loc_map = {"CA":"California", "MI":"Michigan", "NC":"North Carolina", "IL":"Illinois", "TX":"Texas", "NJ":"New Jersey", "WA":"Washington", "MA":"Massachusetts", "TN":"Tennessee", "PA":"Pennsylvania"}

        def text(elt):
            return elt.xpath('./text()')
            
        url2 = response.meta['url2']
        url3 = response.meta['url3']
        url4 = response.meta['url4']
        url5 = response.meta['url5']


        headers = response.meta['headers']
        data = response.meta['data']
        company = response.meta['company']

        save_path = 'C:/Users/training/Documents/SR Hackathon/new_scraper/new_scraper/spiders/'
        

        try:
            founded = response.xpath('//td[text()="Founded"]/following::td[1]//text()').get()
            if founded != None:
                data['Founded'] = founded
            else:
                data['Founded'] =''
        except:
            data['Founded'] = ''

        try:
            indus = response.xpath('//td[text()="Industry"]/following::td[1]//text()').get()
            if indus != None:
                data['Industry'] = indus
            else:
                data['Industry'] =''
        except:
            data['Industry'] = ''

        try:
            sector = response.xpath('//td[text()="Sector"]/following::td[1]//text()').get()
            if sector != None:
                data['Sector'] = sector
            else:
                data['Sector'] =''
        except:
            data['Sector'] = ''

        try:
            emp = response.xpath('//td[text()="Employees"]/following::td[1]//text()').get()
            if emp != None:
                data['Number of Employees'] = emp
            else:
                data['Number of Employees'] =''
        except:
            data['Number of Employees'] = ''

        try:
            ceo = response.xpath('//td[text()="CEO"]/following::td[1]//text()').get()
            if ceo != None:
                data['CEO'] = ceo
            else:
                data['CEO'] =''
        except:
            data['CEO'] = ''


        try:
            tics = response.xpath('//td[text()="Ticker Symbol"]/following::td[1]//text()').get()
            if tics != None:
                data['Ticker Symbol'] = tics
            else:
                data['Ticker Symbol'] =''
        except:
            data['Ticker Symbol'] = ''


        try:
            isin = response.xpath('//td[text()="ISIN Number"]/following::td[1]//text()').get()
            if isin != None:
                data['ISIN'] = isin
            else:
                data['ISIN'] =''
        except:
            data['ISIN'] = ''
        
        # for table in response.xpath('//table'):
        #     if "Name" in table.xpath('//tr').xpath('//td').xpath('.//text()'):
        #         print(table.xpath('//tr').xpath('//td').xpath('.//text()'))
        #     # if "Position" in table and "Name" in table:
        #     #     print(table)



        yield scrapy.Request(url=url3, callback=self.parse_company3, meta={'headers':headers, 'data':data, 'url2':url2, 'url3':url3, 'url4':url4, 'url5':url5, 'company':company})


    def parse_company3(self, response):

        headers = response.meta['headers']
        data = response.meta['data']
        company = response.meta['company']

        loc_map = {"CA":"California", "MI":"Michigan", "NC":"North Carolina", "IL":"Illinois", "TX":"Texas", "NJ":"New Jersey", "WA":"Washington", "MA":"Massachusetts", "TN":"Tennessee", "PA":"Pennsylvania"}

        def text(elt):
            return elt.xpath('./text()')
            
        url2 = response.meta['url2']
        url3 = response.meta['url3']
        url4 = response.meta['url4']
        url5 = response.meta['url5']


        headers = response.meta['headers']
        data = response.meta['data']
        company = response.meta['company']

        save_path = 'C:/Users/training/Documents/SR Hackathon/new_scraper/new_scraper/spiders/'


        try:
            capexp = response.xpath('//span[@class="ttt" and text()="Capital Expenditures"]/following::td[2]//text()').get()
            if capexp != None:
                data['Capital expenditure'] = capexp
            else:
                data['Capital expenditure'] =''
        except:
            data['Capital expenditure'] = ''

        
        yield scrapy.Request(url=url4, callback=self.parse_company4, meta={'headers':headers, 'data':data, 'url2':url2, 'url3':url3, 'url4':url4, 'url5':url5, 'company':company})



    def parse_company4(self, response):

        headers = response.meta['headers']
        data = response.meta['data']
        company = response.meta['company']

        loc_map = {"CA":"California", "MI":"Michigan", "NC":"North Carolina", "IL":"Illinois", "TX":"Texas", "NJ":"New Jersey", "WA":"Washington", "MA":"Massachusetts", "TN":"Tennessee", "PA":"Pennsylvania"}

        def text(elt):
            return elt.xpath('./text()')
            
        url2 = response.meta['url2']
        url3 = response.meta['url3']
        url4 = response.meta['url4']
        url5 = response.meta['url5']


        headers = response.meta['headers']
        data = response.meta['data']
        company = response.meta['company']

        save_path = 'C:/Users/training/Documents/SR Hackathon/new_scraper/new_scraper/spiders/'


        try:
            markcap = response.xpath('//span[@class="ttt" and text()="Market Capitalization"]/following::td[2]//text()').get()
            if markcap != None:
                data['Market Capitization'] = markcap
            else:
                data['Market Capitization'] =''
        except:
            data['Market Capitization'] = ''

        
        yield scrapy.Request(url=url5, callback=self.parse_company5, meta={'headers':headers, 'data':data, 'url2':url2, 'url3':url3, 'url4':url4, 'url5':url5, 'company':company})


    def parse_company5(self, response):

        headers = response.meta['headers']
        data = response.meta['data']
        company = response.meta['company']

        url2 = response.meta['url2']
        url3 = response.meta['url3']
        url4 = response.meta['url4']
        url5 = response.meta['url5']


        loc_map = {"CA":"California", "MI":"Michigan", "NC":"North Carolina", "IL":"Illinois", "TX":"Texas", "NJ":"New Jersey", "WA":"Washington", "MA":"Massachusetts", "TN":"Tennessee", "PA":"Pennsylvania"}

        def text(elt):
            return elt.xpath('./text()')
            


        headers = response.meta['headers']
        data = response.meta['data']
        company = response.meta['company']

        save_path = 'C:/Users/training/Documents/SR Hackathon/new_scraper/new_scraper/spiders/'

        try:
            longdbt = response.xpath('//span[@class="ttt" and text()="Long-Term Debt"]/following::td[2]//text()').get()
            if longdbt != None:
                data['Long term Debt'] = longdbt
            else:
                data['Long term Debt'] =''
        except:
            data['Long term Debt'] = ''


        try:
            comst = response.xpath('//span[@class="ttt" and text()="Common Stock"]/following::td[2]//text()').get()
            if comst != None:
                data['Common Stock Value'] = comst
            else:
                data['Common Stock Value'] =''
        except:
            data['Common Stock Value'] = ''

        



        





        if not os.path.exists(save_path):
            os.makedirs(save_path)
        with open(os.path.join(save_path, 'extract2'+".json"), 'a') as out_file_json:
            out_json = json.dumps(data)
            out_file_json.write(out_json+"\n")

        with open(os.path.join(save_path, 'extract2'+".csv"), 'a', newline='') as out_file_csv:
            writer = csv.writer(out_file_csv)
            writer.writerow(list(data.values()))



# //th[text()="Position"]/following::td//text()