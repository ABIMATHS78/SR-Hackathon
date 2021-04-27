import scrapy
import os
import json
import pandas as pd
import csv

class CsimarketSpider(scrapy.Spider):
    name = 'csimarket'

    def __init__(self, *a, **kw):
        super(CsimarketSpider, self).__init__(*a, **kw)

    def start_requests(self):
        base_url = 'https://csimarket.com/stocks/at_glance.php?code='
        base_url2 = 'https://csimarket.com/stocks/income.php?code='
        base_url3 = 'https://csimarket.com/stocks/balance.php?code='
        company_dict = {"Apple Inc":"AAPL", "Kellogg Company":"K", "Visa Inc":"V", "Wells Fargo and Company":"WFC", "Bank Of America Corporation":"BAC", "United Airlines Holdings Inc":"UAL", "Ford Motor Co":"F", "Exxon Mobil Corporation":"XOM", "Chevron Corp":"CVX", "Johnson and Johnson":"JNJ", "Amazon Com Inc":"AMZN", "Abbvie inc":"ABBV", "Biogen Inc":"BIIB", "Fedex Corporation":"FDX", "Hershey Co":"HSY", "Tesla Inc":"TSLA"}

        headers = ["Market Capitalization", "Shares Outstanding", "Number of Employees", "Revenues", "Net Income", "Cash Flow", "Capital Exp.", "Name of Company", "Location", "Sector", "Sub-Sector", "30 Day Performance", "52 Week Average", "Operating Expenses", "Ticker Symbol", "Common Stock Value"]
        
        save_path = 'C:/Users/training/Documents/SR Hackathon/new_scraper/new_scraper/spiders/'

        if not os.path.exists(save_path):
            os.makedirs(save_path)
        # with open(os.path.join(save_path, 'extract'+".csv"), 'a') as out_file_csv:
        #     # creating a csv dict writer object 
        #     writer = csv.DictWriter(out_file_csv, fieldnames = headers)
        #     # writing headers (field names) 
        #     writer.writeheader()

        # with open(os.path.join(save_path, 'extract'+".csv"), 'a') as out_file_csv: 
        #     # i = 1
        #     for line in headers:
        #         try:
        #             out_file_csv.write(line)
        #         except:
        #             out_file_csv.write('')
        #         out_file_csv.write(',')
        #         # if i < len(headers)-1:
        #         #     out_file_csv.write(',')
        #         # i += 1
        #     out_file_csv.write('')
        #     out_file_csv.write('\n')
        with open(os.path.join(save_path, 'extract'+".csv"), 'a', newline='') as out_file_csv:
            writer = csv.writer(out_file_csv)
            writer.writerow(headers)

        for name, company in company_dict.items():
            url = base_url+str(company)
            url2 = base_url2+str(company)
            url3 = base_url3+str(company)
            yield scrapy.Request(url=url, callback=self.parse_company, meta={'name':name, 'headers':headers, 'url2':url2, 'url3':url3, 'company':company})


    def parse_company(self, response):
        url2 = response.meta['url2']
        url3 = response.meta['url3']
        company = response.meta['company']
        loc_map = {"CA":"California", "MI":"Michigan", "NC":"North Carolina", "IL":"Illinois", "TX":"Texas", "NJ":"New Jersey", "WA":"Washington", "MA":"Massachusetts", "TN":"Tennessee", "PA":"Pennsylvania"}

        def text(elt):
            return elt.xpath('./text()').extract_first(default='')

        text1 = ""
        text2 = ""
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
                        text1 = text(td)
                    else:
                        if i == 2:
                            text2 = text(td).replace("\n", "").lstrip().rstrip()
                        


                    i += 1
                if "Market Capitalization" in text1:
                    try:
                        data["Market Capitalization"] = text2.replace(",", "")
                    except:
                        data["Market Capitalization"] = ''
                elif "Shares" in text1 and "Outstanding" in text1:
                    try:
                        data["Shares Outstanding"] = text2.replace(",", "")
                    except:
                        data["Shares Outstanding"] = ''
                elif "Employees" in text1:
                    try:
                        data["Number of Employees"] = text2.replace(",", "")
                    except:
                        data["Number of Employees"] = ''
                elif "Revenues" in text1:
                    try:
                        data["Revenues"] = response.xpath('//td[@class="al ecol"]/following-sibling::td[1]//text()').get().replace("\n", "").lstrip().rstrip().replace(",", "")
                    except:
                        data["Revenues"] = ''
                elif "Net Income" in text1:
                    try:
                        data["Net Income"] = response.xpath('//td[contains(text(),"Net Income")]/following-sibling::td//text()').get().lstrip().rstrip().replace(",", "")
                    except:
                        data["Net Income"] = ''
                elif "Cash Flow" in text1:
                    try:
                        data["Cash Flow"] = response.xpath('//td[contains(text(),"Cash Flow")]/following-sibling::td//text()').get().lstrip().rstrip().replace(",", "")
                    except:
                        data["Cash Flow"] = ''
                elif "Capital Exp." in text1:
                    try:
                        data["Capital Exp."] = text2.replace(",", "")
                    except:
                        data["Capital Exp."] = ''

        data["Name of Company"] = response.meta['name']

        headers = response.meta['headers']
        # grand [data]
        # df = pd.DataFrame(data)
        # df = df.transpose()
        save_path = 'C:/Users/training/Documents/SR Hackathon/new_scraper/new_scraper/spiders/'

        try:
            data["Location"] = loc_map[response.xpath('//span[@class="lzmt"]/text()').get().lstrip().rstrip()[-2:]]
        except:
            data["Location"] = ""
        i = 1
        for td in response.xpath('//td[@class="wsnw al"]'):
            j = 1
            for lk in td.xpath('a'):
                if j == 1:
                    try:
                        data["Sector"] = lk.xpath('./text()').extract()[-1].lstrip().rstrip()
                    except:
                        data["Sector"] = ''
                else:
                    if j == 2:
                        try:
                            data["Sub-Sector"] = lk.xpath('./text()').extract()[-1].lstrip().rstrip()
                        except:
                            data["Sub-Sector"] = ''
                j += 1

            # print(text(td.xpath('span')))
            # print("_____________", data["Name of Company"])
            # if "Sector" in text(td.xpath('span')):
            #     print(text(td.xpath('a')))

            if i == 1:
                break
        
        try:
            data['30 Day Performance'] = str(float(response.xpath('//td[@class="svjetlirub s"]/span//text()').get().lstrip().rstrip().replace("%", "").replace(" ", ""))/100)
        except:
            data['30 Day Performance'] = ''

        try:
            data['52 Week Average'] = response.xpath('//td[contains(.,"Avg:")]/following-sibling::td//text()').get().lstrip().rstrip().replace("$", "").replace(" ", "")
        except:
            data['52 Week Average'] = ''


        yield scrapy.Request(url=url2, callback=self.parse_company2, meta={'headers':headers, 'data':data, 'url2':url2, 'url3':url3, 'company':company})



    def parse_company2(self, response):

        loc_map = {"CA":"California", "MI":"Michigan", "NC":"North Carolina", "IL":"Illinois", "TX":"Texas", "NJ":"New Jersey", "WA":"Washington", "MA":"Massachusetts", "TN":"Tennessee", "PA":"Pennsylvania"}

        def text(elt):
            return elt.xpath('./text()')
            
        url2 = response.meta['url2']
        url3 = response.meta['url3']
        headers = response.meta['headers']
        data = response.meta['data']
        company = response.meta['company']

        save_path = 'C:/Users/training/Documents/SR Hackathon/new_scraper/new_scraper/spiders/'

    
        # g = response.xpath('//td[text()="Total costs &amp; expenses "]/following::td[1]/span/text()')
        try:
            oper_exp = response.xpath('//td[@class="svjetlirub capital f11"]/following-sibling::td[1]//text()').get().replace(",", "")
            if oper_exp != None:
                data['Operating Expenses'] = oper_exp
            else:
                data['Operating Expenses'] = ""
        except:
            data['Operating Expenses'] = ''
        try:
            data['Ticker Symbol'] = company
        except:
            data['Ticker Symbol'] = ''
        # print(response.xpath('//td[@class="svjetlirub"]/following-sibling::td[1]//text()').get())
        # try:
        #     data['Common Stock Value'] = response.xpath('//td[@class="svjetlirub"]/following-sibling::td[1]//text()').get()
        # except:
        #     data['Common Stock Value'] = ''
        # data['30 Day Performance'] = response.xpath('//td[text()="30 Day Perf:"]/following-sibling::td//text()').get()
        # print(response.xpath('//td[@class="svjetlirub al s"]'))
        # text()="30&nbsp;Day&nbsp;Perf:"
        print(data)

        yield scrapy.Request(url=url3, callback=self.parse_company3, meta={'headers':headers, 'data':data, 'url2':url2, 'url3':url3, 'company':company})




    def parse_company3(self, response):

        loc_map = {"CA":"California", "MI":"Michigan", "NC":"North Carolina", "IL":"Illinois", "TX":"Texas", "NJ":"New Jersey", "WA":"Washington", "MA":"Massachusetts", "TN":"Tennessee", "PA":"Pennsylvania"}

        def text(elt):
            return elt.xpath('./text()')
            


        headers = response.meta['headers']
        data = response.meta['data']
        company = response.meta['company']

        save_path = 'C:/Users/training/Documents/SR Hackathon/new_scraper/new_scraper/spiders/'

    
        # g = response.xpath('//td[text()="Total costs &amp; expenses "]/following::td[1]/span/text()')
        # data['Operating Expenses'] = response.xpath('//td[@class="svjetlirub capital f11"]/following-sibling::td[1]//text()').get()
        # data['Ticker Symbol'] = company
        # print(response.xpath('//td[@class="svjetlirub"]/following-sibling::td[1]//text()').get())
        # data['Common Stock Value'] = response.xpath('//td[@class="svjetlirub"]/following-sibling::td[1]//text()').get()
        try:
            common_sv = response.xpath('//td[text()="  Common Stock Value"]/following-sibling::td[1]//text()').get().replace(",", "")
            print(str(common_sv), "This is it")
            if common_sv != None:
                data['Common Stock Value'] = common_sv
            else:
                data['Common Stock Value'] = ""
        except:
            data['Common Stock Value'] = ''
        # data['30 Day Performance'] = response.xpath('//td[text()="30 Day Perf:"]/following-sibling::td//text()').get()
        # print(response.xpath('//td[@class="svjetlirub al s"]'))
        # text()="30&nbsp;Day&nbsp;Perf:"
        # print(company)
        print(data)
        # print(data['Common Stock Value'])
        # print(data['30 Day Performance'])
        # g = response.xpath('//td[preceding::td[1]/text()="Total costs &amp; expenses "]')
            # m = 1
            # for td in tr.xpath('td'):
            #     if m == 2:
            #         hold = td.xpath('//a'):
            #     m += 1
        






        if not os.path.exists(save_path):
            os.makedirs(save_path)
        with open(os.path.join(save_path, 'extract'+".json"), 'a') as out_file_json:
            out_json = json.dumps(data)
            out_file_json.write(out_json+"\n")
        # with open(os.path.join(save_path, 'extract2'+".json"), 'a') as out_file_json:
        #     out_json = json.dumps(grand)
        #     out_file_json.write(out_json+"\n")
        # with open(os.path.join(save_path, 'extract'+".csv"), 'a') as out_file_csv:
        #     # i = 1
        #     for line in list(data.values()):
        #         try:
        #             out_file_csv.write(line)
        #         except:
        #             out_file_csv.write('')
        #         out_file_csv.write(',')
        #         # if i < len(headers)-1:
        #         #     out_file_csv.write(',')
        #         # i += 1
        #     out_file_csv.write('')
        #     out_file_csv.write('\n')
        with open(os.path.join(save_path, 'extract'+".csv"), 'a', newline='') as out_file_csv:
            writer = csv.writer(out_file_csv)
            writer.writerow(list(data.values()))

        # with open(os.path.join(save_path, 'extract'+".csv"), 'a') as out_file_csv:
        #     # creating a csv dict writer object 
        #     writer = csv.DictWriter(out_file_csv, fieldnames = headers)
        #     # writing headers (field names) 
        #     writer.writeheader()
        #     print([data])
        #     # writing data rows 
        #     writer.writerows([data])

        # # df.to_csv(save_path, index=False)
