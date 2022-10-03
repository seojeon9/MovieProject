from bs4 import BeautifulSoup
import requests
from datetime import datetime
import json
from infra.hdfs_client import get_client
from infra.jdbc import DataWarehouse, find_data

class MovieScoreExtractor:
    std_date=str(datetime.now().date())
    file_dir = '/movie_data/score/'
    cols = ['movie_code', 'title','audi_sc', 'expe_sc', 'neti_sc', 'std_date']

    movie_url_df = find_data(DataWarehouse, 'MOVIE_URL')
    movie_url_df = movie_url_df.drop_duplicates(['MOVIE_CODE'])
    movie_code_list = movie_url_df.select('MOVIE_CODE').rdd.flatMap(lambda x: x).collect()
    movie_url_list = movie_url_df.select('URL').rdd.flatMap(lambda x: x).collect()

    @classmethod
    def extract_data(cls):
        try :
            for i in range(len(cls.movie_code_list)):
                data=[]
                file_name = 'movie_score_' + str(cls.movie_code_list[i]) + '_' + cls.std_date + '.json'
                url = cls.movie_url_list[i]
                html = requests.get(url).content
                soup = BeautifulSoup(html,"html.parser")

                rows=[]
                rows.append(cls.movie_code_list[i])

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
                            'movie_code':'영화코드'
                            ,'title':'영화제목'
                            ,'audi_sc':'관람객평점'
                            ,'expe_sc':'기자및평론가평점'
                            ,'neti_sc':'네티즌평점'
                            ,'std_date':'수집일자'
                        },
                    },
                'data':data
                }

                get_client().write(cls.file_dir+file_name, json.dumps(res, ensure_ascii=False), encoding='utf-8')
        except:
            pass
