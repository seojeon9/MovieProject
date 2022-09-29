from bs4 import BeautifulSoup
import requests
from datetime import datetime
from tqdm import tqdm
import time
import json
from hdfs import InsecureClient

class MovieScoreExtractor:
    client = InsecureClient('http://localhost:9870', user='big')
    url = 'https://movie.naver.com/movie/bi/mi/basic.naver?code=201641'
    std_date=str(datetime.now().date())
    file_dir = '/movie_data/score/'
    cols = ['title','audi_sc', 'expe_sc', 'neti_sc', 'std_date']

    @classmethod
    def extract_data(cls):
        for i in tqdm(range(0,1)):
            data=[]
            file_name = 'movie_score_' + cls.std_date + '.json'

            html = requests.get(cls.url).content
            soup = BeautifulSoup(html,"html.parser")
        #     time.sleep(1)

            rows=[]
            
            try :
                title=soup.findAll("h3",{"class":"h_movie"})[0].text.split('\n')[1]
                rows.append(title)
            except Exception as e:
                rows.append('없음')
                
                
            try :
                audi_sc=soup.findAll("span",{"class":"st_on"})[0].text.split(" ")[2].replace('점',"")
                rows.append(audi_sc)
            except Exception as e:
                rows.append('없음')
                
                
            try :
                expe_sc=soup.findAll("div",{"class":"spc_score_area"})[0].text.split("\n\n")[2]
                rows.append(expe_sc)
            except Exception as e:
                rows.append('없음')
            
            
            try :
                neti_sc=soup.findAll("a",{"id":"pointNetizenPersentBasic"})[0].text
                rows.append(neti_sc)
            except Exception as e:
                rows.append('없음')
            
            rows.append(cls.std_date)

            tmp = dict(zip(cls.cols, rows))
            data.append(tmp)

            res = {
                'meta':{
                    'desc':'네이버 영화 평점 현황',
                    'cols':{
                        'title':'영화제목'
                        ,'audi_sc':'관람객평점'
                        ,'expe_sc':'기자및평론가평점'
                        ,'neti_sc':'네티즌평점'
                        ,'std_date':'수집일자'
                    },
                },
            'data':data
            }

            cls.client.write(cls.file_dir+file_name, json.dumps(res, ensure_ascii=False), encoding='utf-8')
