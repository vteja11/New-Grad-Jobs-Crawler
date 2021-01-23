import scrapy
from bs4 import BeautifulSoup 


class Amazonjobs(scrapy.Spider):
    name = "amazonjobs"

    def start_requests(self):
        urls = [
            'https://www.amazon.jobs/en/jobs/1411781/software-development-leader-cloud-computing-amazon-web-services',
            'https://www.amazon.jobs/en/jobs/1411703/software-development-engineer',
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        soup = BeautifulSoup(response.text, 'html.parser')
        job_d=soup.select('div[class="section"]');

        for section in job_d:
            if(section.h2!=None):
                if(section.h2.get_text()=='BASIC QUALIFICATIONS'):
                    
                    print(section.h2.get_text());
                    print(section.p.get_text());
        