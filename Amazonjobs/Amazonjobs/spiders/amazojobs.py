import scrapy
from bs4 import BeautifulSoup
import time 

from scrapy import signals, Request;                      #pip3 install beautifulsoup4

class AmazonJobsSpider(scrapy.Spider):
    name = "amazonjobs"

    pages_count =25;
    url_count=10;

    job_details=[];
    start="";
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

    def spider_closed(self, spider):
        print("------ spider -- closed");
        #print("--------job---------", self.job_details)
        end_time=time.time()-self.start
        print(" Total time spent ", end_time);
        print(" job details ",len(self.job_details))

    def spider_idle(self, spider):
        print("----- spider idle")

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
                "page_no":self.pages_count+1
            }

            
            #print(EachPart.get_text())
            #print(obj)
            yield scrapy.Request(url=obj["link"], callback=self.parse_jd, meta={'job':obj})
            #go to the link and get job description

            #self.job_details.append(obj);

        print("------------------------obj count------------------------", len(self.job_details))
        url = 'https://www.amazon.jobs/en/search?offset='+str(self.url_count)+'&result_limit=10&sort=recent&category[]=software-development&cities[]=Seattle%2C%20Washington%2C%20USA&cities[]=Bellevue%2C%20Washington%2C%20USA&distanceType=Mi&radius=24km&loc_group_id=seattle-metro&latitude=&longitude=&loc_group_id=seattle-metro&loc_query=Greater%20Seattle%20Area%2C%20WA%2C%20United%20States&base_query=Software%20Development&city=&country=&region=&county=&query_options=&'
        self.url_count=self.url_count+10;


        #print("entering follow links")
        if(self.pages_count>0):
            self.pages_count=self.pages_count-1;
            yield scrapy.Request(url=url, callback=self.parse, meta={'splash':{'args':{'wait':'0.5'},'endpoint':'render.html'}})

        else:
            #print(self.job_details, len(self.job_details));
            return {"obj":"entering follow links"};

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
        return {"onj":"entering job description"};


        


