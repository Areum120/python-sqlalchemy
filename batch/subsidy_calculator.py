# -*- coding: utf-8 -*-
# 한글 인코딩 에러
# 경고창 무시
import warnings

import numpy as np

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

    def crawler_subsidy(self):
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
        # print(driver.window_handles)
        # 새창 핸들링
        driver.switch_to.window(driver.window_handles[-1])
        time.sleep(2)

        # 해당 접속 사이트 url 가져오기
        html = driver.page_source
        # print(html)

        # # table crawling
        soup = bs(html, 'html.parser')
        data = soup.find('table', {'class':'table_02_2_1'})
        # print(data)
        #
        # # 2022 지자체 자종별 보조금 table 갖고 오기
        table = parser_functions.make2d(data)
        # # print(table[0])
        # # print(table[1:])
        self.table = table
        #
        # parsing
        # 모든 열 출력
        pd.set_option('display.max_columns', None)
        # check_subsidy_area 데이터프레임 생성
        df = pd.DataFrame(data=self.table[1:], columns=self.table[0])
        # print(df)
        self.df = df

        # 모델별 지방비 조회
        def sido_car_option(optionNo):
            option_btn = driver.find_element_by_xpath('/ html / body / form / table / tbody / tr['+ optionNo +'] / td[1] / a')
            option_btn.click()#조회 버튼 클릭

            # 모델별 지방비 table 갖고 오기
            html2 = driver.page_source
            soup2 = bs(html2, 'html.parser')
            data2 = soup2.find('table', {'class': 'table_02_2_1'})
            model_table = parser_functions.make2d(data2)
            self.model_table = model_table

            # 모든 열 출력
            pd.set_option('display.max_columns', None)

            # parsing
            df_optionNo = pd.DataFrame(data=self.model_table[1:], columns=self.model_table[0])
            self.df_optionNo = df_optionNo
            # print(df_optionNo)

            # 뒤로가기 버튼 클릭
            back_btn = driver.find_element_by_xpath('/ html / body / form / div / a')
            back_btn.click()

        # 반복문으로 전체 버튼 조회
        option_list = list(range(1,3))#optionNo은 1~161번까지 (1, 162)
        for i in option_list:
            sido_car_option(str(i))
        # example
        # sido_car_option(str(1))

    # 문제 마지막 self.df_optionNo만 나옴, sido_car_option함수 내에서 parsing 후 db에 데이터 넣고, back 버튼을 누르고 다시 불러온 데이터를 넣고 반복해야 할 듯
    
    def subsidy_calculator_insert(self):
        # id, sodo, region, importer, model, num_notice_all, num_remains_all, acceptance_rate_all, car_price, subsidy, sum_price, created_at, updated_at

        # table 생성
        check_subsidy_area_df = self.df_optionNo[['제조사', '모델명', '보조금(만원)']]

        # 데이터프레임 특수문자 제거
        check_subsidy_area_df['sido'] = self.df['시도'].str.replace(pat=r'[^\w]', repl=r'', regex=True)
        check_subsidy_area_df['region'] = self.df['지역구분'].str.replace(pat=r'[^\w]', repl=r'', regex=True)
        check_subsidy_area_df['subsidy'] = check_subsidy_area_df['보조금(만원)'].str.replace(pat=r'[^\w]', repl=r'', regex=True)

         # row 생성 일자
        check_subsidy_area_df['created_at'] = datetime.datetime.now()
        check_subsidy_area_df['updated_at'] = datetime.datetime.now()

        # 첫번째로 id 추가할 때
        id = list(range(0, 61, 1))
        # 9822(61x161)

        # 0번째 칼럼에 id 리스트 추가
        check_subsidy_area_df.insert(0, 'id', id, True)

        print(check_subsidy_area_df)
        #   check_subsidy_area_df.columns =
        #
        # # 콤마 제거
        # check_subsidy_area_df['subsidy_riding'] = check_subsidy_area_df['subsidy_riding'].str.replace(',',"")
        # # print(check_subsidy_area_df.isnull().sum())#결측값 개수#다 0개로 나옴
        # # 빈 문자열을 널값으로 바꾸기
        # check_subsidy_area_df['subsidy_riding'].replace('', np.nan, inplace=True)
        # check_subsidy_area_df['subsidy_compact'].replace('', np.nan, inplace=True)
        # print(check_subsidy_area_df)
        #
        # # 결측값 0으로 채우기
        # check_subsidy_area_df = check_subsidy_area_df.fillna(0)
        #
        # # 결측값 확인
        # print(check_subsidy_area_df.isnull().sum())
        #
        # # type 변환
        # check_subsidy_area_df = check_subsidy_area_df.astype({'subsidy_riding': 'int', 'subsidy_compact': 'int'})
        # print(check_subsidy_area_df)

        # ev_subsidy_calculator table insert

        # try:
        #     subsidy_calculator_df.to_sql(name='ev_subsidy_calculator', con=db.engine, if_exists='append',
        #                                  index=False)  # table이 있는 경우 if_exists='append' 사용, 값을 변경하려면 replace, index는 생성하지 말기
        #     print('check_subsidy_car insert 완료')
        # except Exception as e:
        #     print(e)









# crawler 실행
SI = subsidy_info_insert('https://ev.or.kr/portal/localInfo')
SI.crawler_subsidy()

# table insert(1번만 실행해도 됨)
SI.subsidy_calculator_insert()


