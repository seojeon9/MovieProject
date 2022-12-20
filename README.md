# 영화 산업 흥행 요인 분석을 위한 데이터 파이프라인 구축
Data Engineer 이서정(팀장), 손지수, 유승종, 이상엽, 정현진

프로젝트 기간
2022-09-26~ 2022-10-07 (총 12일)

## 1. 프로젝트 개요

### 1-1 주제 선정 배경
보통 영화를 볼때 좋은 배우, 감독을 보고 영화가 흥행할지 아닐지 판단하곤 합니다. 하지만 좋은 배우, 감독이 참여했음에도 흥행하지 못한 사례들을 볼 수 있습니다. 저희 팀은 이를 보고 영화 내적, 외적인 요인들이 흥행에 어떤 영향을 끼치는지 파악해보고 싶었습니다.

### 1-2 프로젝트 주제
▶ 영화 산업 흥행 요인 분석을 위한 데이터 파이프라인 구축
: 개봉일의 흥행성과가 영화의 최종 흥행에 미치는 영향

### 1-3 수집 데이터
- 영화 진흥 위원회 일별 박스오피스 API : 영화코드, 순위, 매출, 관객수 등등
- 영화 진흥 위원회 영화 상세정보 API : 영화명, 장르, 개봉일 등등
- 네이버 검색어 트렌드 API : 영화 검색 비율
- 네이버 검색 영화 API : 네이버 영화 URL, 출연 배우
- 네이버 영화 평점 정적크롤링 : 네티즌/ 관람객/ 전문가 평점

### 1-4 협업에 사용한 툴
- slack 
- trello : https://trello.com/b/w9hAewpb/de3%EC%A1%B0
- docker : https://hub.docker.com/u/seojeon9

### 1-5 기술 스택
#### ETL 파이프라인
- Data Lake : <img src="https://img.shields.io/badge/Hadoop-66CCFF?style=flat-square&logo=apachehadoop&logoColor=black"> 
- Data Warehouse, Data Mart : <img src="https://img.shields.io/badge/Oracle ATP-F80000?style=flat-square&logo=oracle&logoColor=white">
- 데이터 가공 및 분산처리엔진 : <img src="https://img.shields.io/badge/Spark-E25A1C?style=flat-square&logo=apachespark&logoColor=white">
- 배치도구 : <img src="https://img.shields.io/badge/Airflow-017CEE?style=flat-square&logo=apacheairflow&logoColor=black">
#### REST-API Server
- Web Framework : <img src="https://img.shields.io/badge/Django-092E20?style=flat-square&logo=django&logoColor=white">
#### BI
- 언어 : <img src="https://img.shields.io/badge/html5-E34F26?style=flat-square&logo=html5&logoColor=white"> 
  <img src="https://img.shields.io/badge/css-1572B6?style=flat-square&logo=css3&logoColor=white"> 
  <img src="https://img.shields.io/badge/javascript-F7DF1E?style=flat-square&logo=javascript&logoColor=black"> 
- 프레임워크 : <img src="https://img.shields.io/badge/bootstrap-7952B3?style=flat-square&logo=bootstrap&logoColor=white">

### 1-6 데이터 처리 프로세스
- ETL 파이프라인 
- Data Mart
- REST-API
- BI

## 2. 프로젝트 수행 결과 - 데이터 파이프라인
### 2-1 Data Lake (추출)
![image](https://user-images.githubusercontent.com/72624263/194675149-534b2751-2de2-4bd2-90cf-30143694e1c7.png)
<br>
: 도커 컨테이너 위에 하둡을 얹어 Data Lake로 사용하며 필요한 데이터 추출.
### 2-2 가공
![image](https://user-images.githubusercontent.com/72624263/194675199-9b679007-a5cf-4d3b-802f-dbbf94ab073e.png)
<br>
: HDFS에 추출한 Json파일을 spark를 사용하여 구축해 놓은 데이터베이스에 맞게 가공.
### 2-3 Data Warehouse
![image](https://user-images.githubusercontent.com/72624263/194675298-529e69d1-524d-43cf-aade-ad4eefce6461.png)
<br>
: https://www.erdcloud.com/d/6CcF67mjXiuPBogSD <br>
데이터 웨어하우스의 테이블을 설계할때 고려한 점은 중복값없이 이상현상이 생기지 않게 저장을 잘 해야한다는 것.<br>
그래서 3정규화까지 고려하여 진행.
![image](https://user-images.githubusercontent.com/72624263/194676044-b9063fbe-7385-4c80-bed0-d70ac38c1031.png)
<br>
: 영화 상세정보의 경우 영화 한편에 대하여 장르가 멜로/로맨스와 코미디 두가지고 나타남. 컴퍼티 파라미터도 하나가 아닌 여러값을 보내줌.<br>
이런값들을 처리하기 위해 MOVIE_DETAIL과 MOVIE_GENRE, MOVIE_COMPANY와 같이 테이블을 나눠 일대다의 관계로 테이블을 설계.
### 2-4 Data Mart
![image](https://user-images.githubusercontent.com/72624263/194676150-77148ee0-53c3-4641-9806-027cf7dd2ea1.png)
<br>
: https://www.erdcloud.com/d/ARnEaHq27AZCkXXx6
분석에 사용할 데이터를 적재할 데이터마트를 설계할 때 고려한 점은 각 요인에 따라 테이블을 나누고 데이터가 중복이 되더라도 하나의 테이블에서 원하는 정보를 다 얻을 수 있도록 설계하는 것. <br>
또한 데이터 웨어하우스는 로우한 데이터들이 각 행을 이룬다면 데이터 마트에는 각 로우한 값의 통계 데이터가 들어갈 수 있게 설계.<br>
통계 데이터로는 평균과 증가율을 사용.<br>
자체적으로 총 관객수를 기준으로 A부터 F까지 영화의 등급을 매겨서 테이블의 컬럼으로 추가. -> 각 요소들간의 상관관계를 분석 가능<br>
영화가 가진 고유한 특성을 의미하는 내부적 요인, 날이나 환경에 따라 변하는 외부적 요인도 구분하여 테이블 설계.
### 2-5 Airflow
![image](https://user-images.githubusercontent.com/72624263/194676491-683b4f9b-f5ff-49c4-8aae-0344e7525a8e.png)
<br>
: 순차적으로 진행해야하는 스크립트의 경우 직렬처리. spark session을 사용해야하는 transform의 경우에도 직렬처리 함. 마지막 Data mart는 병렬처리하여 빠르게 끝낼 수 있도록 함.<br>
매일 새벽 airflow를 돌려 자동으로 데이터를 수집, 적재, 가공 할 수 있게 함.
## 3. 프로젝트 수행 결과 - 데이터 프로덕트
### 3-1 REST-API
![image](https://user-images.githubusercontent.com/72624263/194676638-a3d4d46a-43a0-4847-a0f6-1a9fec2d476e.png)
<br>
데이터를 제공하는 백엔드와 로그인정보 세션 등을 관리하는 프론트 서버로 이루어진 분리구조로 구성. <br>
토큰기반 인증을 통한 보안설정. <br>
백엔드 서버에서 데이터베이스의 데이터를 직접 제공하는 것이 아닌 JSON형식으로 제공. <br>
### 3-2 BI
![image](https://user-images.githubusercontent.com/72624263/194677120-30ace656-a29a-4bfe-981d-54b4f5afa4e3.png)
![image](https://user-images.githubusercontent.com/72624263/194677137-f6b2e75c-5565-4022-9dcd-77524089be36.png)
![image](https://user-images.githubusercontent.com/72624263/194677195-90209ad3-ac21-45e5-8b07-b831a1ee1197.png)
<br>
plotly.js 프레임워크를 이용하여 데이터를 시각화.<br>
각 등급별로 개봉일의 요소들과 총 관객수의 상관관계를 확인할 수 있는 웹페이지 구현.

## 4. 마무리
### 4-1 느낀점
이서정 : 처음 접한 개념들과 기술을 이용해 팀플을 진행하니 정말 어려우면서도 함께 해나가는게 재밌었습니다. 시간이 조금 더 주어졌으면 하는 아쉬움이 남지만 많이 배운 시간이었던 것 같습니다. <br>
손지수 : 데이터 파이프라인의 여러 과정들이 어떻게 연결되어 있는지 배울 수 있어 뜻깊었습니다. 백엔드와 프론트엔드의 지식이 부족해 원하는 시각화를 쉽게 구현할 수 없어 아쉬웠다. 팀원에게 배우면서 부족한 부분을 파악하고 공부할 수 있어 감사했습니다. <br>
유승종 : 만들어진 데이터를 가져다 쓰다가 직접 ETL과정을 거쳐 데이터를 수집해보니 초기 설계나 정규화 과정 등 중요하게 생각할 것들을 보며 따라갈 수 있는 좋은 기회였습니다. <br>
이상엽 : 코드 한줄을 읽거나 쓰는것도 거의 못했는데 이번 팀원분들 덕분에 조금을 방법을 배운 것 같습니다. 많이 어려웠으나 많이 배웠고 꾸준히 하려 제것으로 만들겠습니다. <br>
정현진 : 처음 배운 내용으로 프로젝트를 진행하여니 막막하고 어려움이 있었습니다. 하지만 서로 알려주고 배우며 최선을 다해 진행한, 가장 이상적인 협업을 한 것 같다. 협업에 대한 두려움을 없앨 수 있는 경헙이었습니다. <br>






