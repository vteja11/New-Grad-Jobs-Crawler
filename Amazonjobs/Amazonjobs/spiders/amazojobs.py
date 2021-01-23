import scrapy
from bs4 import BeautifulSoup
import time #pip3 install beautifulsoup4
from elasticsearch import Elasticsearch, helpers;
from scrapy import signals, Request;                      #pip3 install beautifulsoup4
import configparser;            #pip3 install configparser
import os;
import uuid;                    #pip install uuid
from pytz import timezone
from datetime import datetime;

class AmazonJobsSpider(scrapy.Spider):
    name = "amazonjobs"

    pages_count =250;
    curr_page=1;
    url_count=10;

    job_details=[];
    start="";

    es="";
    currentdate="MMMM-DD-YY"

    upload_data=[]=""

    indexName="amazonjobs"
    index="amazon";
    def start_requests(self):
        urls = [
            'https://www.amazon.jobs/en/search?offset=0&result_limit=10&sort=recent&category[]=software-development&cities[]=Seattle%2C%20Washington%2C%20USA&cities[]=Bellevue%2C%20Washington%2C%20USA&distanceType=Mi&radius=24km&loc_group_id=seattle-metro&latitude=&longitude=&loc_group_id=seattle-metro&loc_query=Greater%20Seattle%20Area%2C%20WA%2C%20United%20States&base_query=Software%20Development&city=&country=&region=&county=&query_options=&',
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse, meta={'splash':{'args':{'wait':'0.5'},'endpoint':'render.html'}})



    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(AmazonJobsSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        crawler.signals.connect(spider.spider_idle, signal=signals.spider_idle)
        return spider

    def spider_opened(self, spider):
        self.start=time.time();
        print("--spider---opened");
        spider.logger.info('Spider Opened custom: %s', spider.name)
        config = configparser.ConfigParser();
        path=os.path.join(os.path.dirname(__file__), 'config.txt');
        config.read(path);

        western = timezone('US/Pacific')
        western_time = datetime.now(western)
        pacific=western_time.strftime('%Y-%m-%d----%H-%M-%S')
        #self.currentdate=datetime.today().strftime('%d-%m-%Y');
        self.currentdate = pacific
        elastic_keys_val=['es_host','es_username','es_password', 'es_scheme', 'es_port']

        elastic_config = self.get_config(config, 'es section', elastic_keys_val)

        self.es = self.init_elastic_search(elastic_config[0], elastic_config[1], elastic_config[2], elastic_config[3], elastic_config[4] );

        self.es.indices.delete(index=self.indexName+'*', ignore=[400, 404])


        db = self.indexName.lower()
        self.index = self.indexName.lower()+"-"+self.currentdate;
       

    def spider_closed(self, spider):
        print("------ spider -- closed");
        #print("--------job---------", self.job_details)
        end_time=time.time()-self.start
        print(" Total time spent ", end_time);
        print(" job details ",len(self.job_details))

        self.upload_data=self.job_details
        self.upload_bulk_data();
        self.es.indices.update_aliases({
              "actions": [ 
                { "remove": { "index": self.indexName+"*", "alias": self.indexName }}, 
                { "add":    { "index": self.index, "alias": self.indexName }}
              ]
        })
        self.es.transport.close();

    def spider_idle(self, spider):
        print("----- spider idle")


    def init_elastic_search(self, domain, username, password, scheme, port):
        try:
            es_client= Elasticsearch([domain], http_auth=(username, password), scheme=scheme, port=port);
            self.logger.info(" Opened Elasticsearch Connection");
            return es_client;

        except Exception as e:
            self.logger.error(e);
            return None;

    def get_config(self, config, key, args):
        try:
            res=[]
            for i in args:
                res.append(config.get(key, i));

            return res;

        except Exception as e:
            self.logger.error(e);
            print(e);
    

    def upload_bulk_data(self):
        self.logger.info("uplo")
        if len(self.upload_data)==0:
            self.logger.info("Upload data is 0");

        else:
            try:
                
                #DELETE INDEX

                self.logger.info(" Inserting data in to elasticsearch-index "+self.index);
                helpers.bulk(self.es, self.generate_bulk_data(self.index))

                #update index in the alais

            except Exception as e:
                self.logger.error(e);
                print(e)


    def generate_bulk_data(self, index):

        for doc in self.upload_data:

            yield{
                "_index": index,
                "_type": 'search document',
                "_id": uuid.uuid4(),
                "_source": doc
            }

    def parse(self, response):   
        soup = BeautifulSoup(response.text, 'html.parser')
        

        for script in soup(["script", "style", "noscript"]):
            script.decompose();
        
        #text = soup.get_text()

        jobs=soup.select('div[class="job-tile"]');
        print(len(jobs));
        for EachPart in jobs:
            #link
            link = EachPart.find('a', href=True);
            #print(link['href'])
            #title <h3 class-"job-title">
            title =EachPart.select('h3[class="job-title"]')
            #print(title[0].get_text())
            # location and id
            # <p class="location-and-id"
            location = EachPart.select('p[class*="location"]')
            tmp = location[0].get_text().split("|");
            location =tmp[0];
            job_id=tmp[1].split(":")[1];
            #print(location[0].get_text())
            #posting date
            #<h2 class="posting-date">
            #<p class="meta time-elapsed">
            posting_date=EachPart.select('h2[class = "posting-date"]')
            #print(posting_date[0].get_text())
            
            
            obj={
                "link":'https://www.amazon.jobs'+link['href'],
                "title":title[0].get_text(),
                "location":location,
                "posting_date":posting_date[0].get_text(),
                "basic":"",
                "job_id":job_id,
                "page_no":self.curr_page+1
            }

            
            #print(EachPart.get_text())
            #print(obj)
            yield scrapy.Request(url=obj["link"], callback=self.parse_jd, meta={'job':obj})
            #go to the link and get job description

            #self.job_details.append(obj);

        print("------------------------obj count------------------------", len(self.job_details))
        url = 'https://www.amazon.jobs/en/search?offset='+str(self.url_count)+'&result_limit=10&sort=recent&category[]=software-development&cities[]=Seattle%2C%20Washington%2C%20USA&cities[]=Bellevue%2C%20Washington%2C%20USA&distanceType=Mi&radius=24km&loc_group_id=seattle-metro&latitude=&longitude=&loc_group_id=seattle-metro&loc_query=Greater%20Seattle%20Area%2C%20WA%2C%20United%20States&base_query=Software%20Development&city=&country=&region=&county=&query_options=&'
        self.url_count=self.url_count+10;

        self.upload_data=self.job_details;
        if(len(self.upload_data)>150):
            self.upload_bulk_data();
            self.job_details=[]

        #print("entering follow links")
        if(self.pages_count>0):
            self.pages_count=self.pages_count-1;
            self.curr_page=self.curr_page+1;
            yield scrapy.Request(url=url, callback=self.parse, meta={'splash':{'args':{'wait':'0.5'},'endpoint':'render.html'}})

        else:
            #print(self.job_details, len(self.job_details));
            return None;

    def parse_jd(self, response):
        obj=response.meta.get('job');

        soup = BeautifulSoup(response.text, 'html.parser')
        job_d=soup.select('div[class="section"]');

        basic="";
        
        for section in job_d:
            if(section.h2!=None):
                if(section.h2.get_text()=='BASIC QUALIFICATIONS'):
                    #print(section.p.get_text());
                    basic=section.p.get_text();

        obj["basic"]=basic;
        self.job_details.append(obj);

        #print("- ---- - entering job description")
        return None;


        


