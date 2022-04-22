# -*- coding: utf-8 -*-
# 한글 인코딩 에러
# 경고창 무시
import warnings
warnings.filterwarnings('ignore')
# crawling
from selenium import webdriver
import time
# parsing
from bs4 import BeautifulSoup as bs
from html_table_parser import parser_functions#호출한 data 표형식으로 보기
import pandas as pd
import datetime
# object 연결
from model import models
# db session 연결
import db

class subsidy_info_insert():
    def __init__(self, url):
        self.url = url

    def crawler(self):
        # selenium을 이용한 제어
        # 옵션 생성
        options = webdriver.ChromeOptions()
        options.add_argument('headless')#창 띄우지 않기

        # selenium_webdriver 위치 지정
        driver = webdriver.Chrome('C:\\Users\\workscombine\\Desktop\\work\\study\\evsa_macro\\venv\\Lib\\site-packages\\chromedriver_autoinstaller\\99\\chromedriver', options=options)
        # selenium_driver로 url 접속
        driver.get(self.url)
        # selenium 접속하는데 1초 대기
        time.sleep(1)
        # 지자체 차종별 보조금 클릭하기 위한 x_path
        x_path='//*[@id="btn_car_price"]'
        btn = driver.find_element_by_xpath(x_path)
        # 버튼 클릭
        btn.click()
        time.sleep(2)
        # 탭 목록 확인
        print(driver.window_handles)
        # 새창 핸들링
        driver.switch_to.window(driver.window_handles[-1])
        time.sleep(2)

        # 해당 접속 사이트 url 가져오기
        html = driver.page_source
        # print(html)

        # table crawling
        soup = bs(html, 'html.parser')
        data = soup.find('table', {'class':'table_02_2_1'})
        # print(data)
        # table 갖고 오기
        table = parser_functions.make2d(data)
        # print(table[0])
        # print(table[1:])
        self.table = table

    def crawler_parsing(self):
        # 모든 열 출력
        pd.set_option('display.max_columns', None)
        # 데이터프레임 생성
        df = pd.DataFrame(data=self.table[1:], columns=self.table[0])
        # print(df)
        self.df = df

    def check_subsidy_area(self):
        # 지자체 보조금 table 생성
        check_subsidy_area_df = self.df[['시도', '지역구분', '보조금/승용(만원)(국비+지방비)', '보조금/초소형(만원)(국비+지방비)']]
        # 데이터프레임 특수문자 제거
        check_subsidy_area_df['sido'] = self.df['시도'].str.replace(pat=r'[^\w]', repl=r'', regex=True)
        check_subsidy_area_df['region'] = self.df['지역구분'].str.replace(pat=r'[^\w]', repl=r'', regex=True)
        check_subsidy_area_df['subsidy_riding'] = self.df['보조금/승용(만원)(국비+지방비)'].str.replace(pat=r'[^\w]', repl=r'',regex=True)
        check_subsidy_area_df['subsidy_compact'] = self.df['보조금/초소형(만원)(국비+지방비)'].str.replace(pat=r'[^\w]',repl=r'',regex=True)

         # row 생성 일자
        check_subsidy_area_df['created_at'] = datetime.datetime.now()
        check_subsidy_area_df['updated_at'] = datetime.datetime.now()

        # 첫번째로 id 추가할 때
        id = list(range(0, 161, 1))

        # 0번째 칼럼에 id 리스트 추가
        check_subsidy_area_df.insert(0, 'id', id, True)

        check_subsidy_area_df = check_subsidy_area_df[['id', 'sido', 'region', 'subsidy_riding', 'subsidy_compact', 'created_at', 'updated_at']]

        check_subsidy_area_df['subsidy_riding'] = check_subsidy_area_df['subsidy_riding'].str.replace(',',"")
        # print(check_subsidy_area_df.isnull().sum())#결측값 개수#다 0개로 나옴
        # 빈 문자열 


        # # type 변환
        # check_subsidy_area_df = check_subsidy_area_df.astype({'subsidy_riding': 'int', 'subsidy_compact': 'int'})
        # print(check_subsidy_area_df)

        # subsidy_closing_area table insert
        # try:
        #     check_subsidy_area_df.to_sql(name='check_subsidy_area', con=db.engine, if_exists='append',
        #                                    index=False)  # table이 있는 경우 if_exists='append' 사용, 값을 변경하려면 replace, index는 생성하지 말기
        #     print('check_subsidy_area insert 완료')
        # except Exception as e:
        #     print(e)


# crawler 실행
SI = subsidy_info_insert('https://ev.or.kr/portal/localInfo')
SI.crawler()
SI.crawler_parsing()

# table insert(1번만 실행해도 됨)
SI.check_subsidy_area()
