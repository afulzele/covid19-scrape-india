import boto3
import scrapy
import json
from scrapy.crawler import CrawlerProcess

s3 = boto3.client('s3')

BUCKET = 'covid-tracker-801101744'
KEY = 'handle-csv/covid-india.csv'


class ScaperCovid(scrapy.Spider):
    name = 'scrape_all'
    start_urls = [
        'https://api.covid19india.org/data.json'
    ]

    state_district_dict = {}

    def parse(self, response, s_d_dict=state_district_dict):
        rows = response.body
        data = json.loads(rows)

        statewise = (data.get("statewise"))

        for states in statewise:
            get_state_name = states.get("state")
            s_d_dict[get_state_name] = {"_cases_": states.get("confirmed"), "_deaths_": states.get("deaths"),
                                        "_recovered_": states.get("recovered"), "_district_": {}}

        yield response.follow('https://api.covid19india.org/state_district_wise.json', callback=self.parse_core)

    def parse_core(self, response, s_d_dict=state_district_dict):
        rows = response.body
        data = json.loads(rows)

        for k, v in data.items():
            dist = v.get("districtData")
            for k1, v1 in dist.items():
                if k in s_d_dict and "confirmed" in v1:
                    s_d_dict.get(k).get('_district_')[k1] = v1.get("confirmed")

        for key, value in s_d_dict.items():
            yield {
                "place" : key,
                "cases" : value.get('_cases_'),
                "new_cases" : 0,
                "total_cases" : 0,
                "deaths" : value.get('_deaths_'),
                "new_deaths": 0,
                "total_deaths": 0,
                "recovered" : value.get('_recovered_'),
                'region': value.get('_district_')
            }



def main(event, context):

    # ---------------------------S3-----------------------------

    res = s3.list_objects_v2(Bucket=BUCKET, Prefix=KEY, MaxKeys=1)
    if 'Contents' in res:
        print(res)
        s3.delete_object(Bucket=BUCKET, Key=KEY)

    process = CrawlerProcess({
        'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)',
        'FEED_FORMAT': 'csv',
        'FEED_URI': 's3://'+BUCKET+'/'+KEY
        # 'FEED_URI': 'result.csv'
    })

    process.crawl(ScaperCovid)
    process.start()

    print('All done !')


if __name__ == "__main__":
    main('', '')