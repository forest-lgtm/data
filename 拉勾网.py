import requests
import re
import json
import csv
from hdfs import InsecureClient

headers = {
    'Cookie': 'RECOMMEND_TIP=true; privacyPolicyPopup=false; user_trace_token=20230906143210-2158b2ac-5646-41d3-9382-d932b5975211; LGUID=20230906143210-61e224ab-dd49-4bf8-8a4c-1e9ed30dede5; _ga=GA1.2.1028381936.1693923531; index_location_city=%E5%85%A8%E5%9B%BD; gate_login_token=v1####763570113a84f958a9c49e1332b3fc138d847e219e65bf4666782a1ca95f3b5f; LG_LOGIN_USER_ID=v1####4b2e6aa4580a7477518c01b468085db0ede9dc63c135d1fc48316e0fbafe727d; LG_HAS_LOGIN=1; showExpriedIndex=1; showExpriedCompanyHome=1; showExpriedMyPublish=1; hasDeliver=0; Hm_lvt_4233e74dff0ae5bd0a3d81c6ccf756e6=1693923531,1694071038; _ga_DDLTLJDLHH=GS1.2.1694071038.2.1.1694071126.60.0.0; X_HTTP_TOKEN=5e9e91e04da734c08083954961cfc82e972de2d115; __lg_stoken__=f82b8956d4d983501ee619eead6bfff2eeb1c34dfe46ac2d8638ad80a0b899bff6cad2fe9226d63a88e0f83108d15efc99cc3bcc6e4c25168507e84f89298293001aa95c6fe8; WEBTJ-ID=20230913163009-18a8da97d07fea-0753537564beb2-26021e51-1327104-18a8da97d08f90; JSESSIONID=ABAAAECAAEBABII901B3E968E8E5BB39331EE88FDEA1068; sensorsdata2015session=%7B%7D; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%2226452692%22%2C%22first_id%22%3A%2218a65b5da594ae-0a8bdc4307580c-26021e51-1327104-18a65b5da5a1052%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%2C%22%24latest_referrer%22%3A%22%22%2C%22%24os%22%3A%22Windows%22%2C%22%24browser%22%3A%22Chrome%22%2C%22%24browser_version%22%3A%22107.0.0.0%22%7D%2C%22%24device_id%22%3A%2218a65b5da594ae-0a8bdc4307580c-26021e51-1327104-18a65b5da5a1052%22%7D',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36',
}

with open('lagou_jobs.csv', 'w', encoding='utf-8-sig', newline='') as f:
    writer = csv.writer(f)
    # 写入表头信息
    writer.writerow(['职位名字', '公司名字', '工作城市', '学历要求', '经验要求', '薪资待遇', '公司地址'])

    for i in range(1, 31):
        url = f'https://www.lagou.com/wn/jobs?labelWords=&fromSearch=true&suginput=&kd=python&pn={i}'

        response = requests.get(url=url, headers=headers)
        html_data = re.findall('<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', response.text)[0]
        json_data = json.loads(html_data)

        for index in json_data['props']['pageProps']['initData']['content']['positionResult']['result']:
            data_list = [
                index['positionName'],
                index['companyFullName'],
                index['city'],
                index['education'],
                index['workYear'],
                index['salary'],
                index['positionAddress']
            ]
            writer.writerow(data_list)

print('数据已保存到本地！')
# 将CSV文件上传到HDFS
client = InsecureClient('http://192.168.10.102:9870', user='root')
client.upload('/test/lagou_jobs.csv', 'lagou_jobs.csv')
print('文件上传成功！')





