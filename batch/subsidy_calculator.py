# -*- coding: utf-8 -*-
# 한글 인코딩 에러
# 경고창 무시
import requests as req
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

# db
import db

# 전기차 보조금 계산기 crawler
# 항상 최신 버젼의 접수율 데이터를 받아와야 하기 때문에 crawling한 데이터 replace insert하기

class subsidy_info_insert():
    def __init__(self, url, req):
        self.url = url
        self.req = req

    def crawler_notice(self):
        req = self.req.get(self.url)
        soup = bs(req.text, 'html.parser')
        data = soup.find('table', {'class': 'table_02_2_1'})
        # print(data)
        # table 갖고 오기
        table = parser_functions.make2d(data)
        # print(table[1:])
        # print(table[0])
        self.table = table

    def crawler_notice_parsing(self):
        # parsing
        # 모든 열 출력
        pd.set_option('display.max_columns', None)
        # 데이터프레임
        df = pd.DataFrame(data=self.table[2:], columns=self.table[0])
        # print(df)

        # 민간공고대수 칼럼 parsing
        split_1 = df.민간공고대수.str.split(' ')

        # series->dataframe
        split_1 = split_1.apply(lambda x: pd.Series(x))

        # 컬럼명지정
        split_1.columns = ['민간공고대수_전체', '민간공고대수_우선순위', '민간공고대수_법인기관', '민간공고대수_택시', '민간공고대수_우선비대상']

        # 괄호값 제거(정규식 특수문자 제거)
        split_1['민간공고대수_우선순위'] = split_1['민간공고대수_우선순위'].str.replace(pat=r'[^\w]', repl=r'', regex=True)
        split_1['민간공고대수_법인기관'] = split_1['민간공고대수_법인기관'].str.replace(pat=r'[^\w]', repl=r'', regex=True)
        split_1['민간공고대수_택시'] = split_1['민간공고대수_택시'].str.replace(pat=r'[^\w]', repl=r'', regex=True)
        split_1['민간공고대수_우선비대상'] = split_1['민간공고대수_우선비대상'].str.replace(pat=r'[^\w]', repl=r'', regex=True)

        # 접수대수 칼럼 parsing
        split_2 = df.접수대수.str.split(' ')
        # series->dataframe
        split_2 = split_2.apply(lambda x: pd.Series(x))
        # 컬럼명지정
        split_2.columns = ['접수대수_우선순위', '접수대수_법인기관', '접수대수_택시', '접수대수_우선비대상']

        # 괄호값 제거(정규식 특수문자 제거)
        split_2['접수대수_우선순위'] = split_2['접수대수_우선순위'].str.replace(pat=r'[^\w]', repl=r'', regex=True)
        split_2['접수대수_법인기관'] = split_2['접수대수_법인기관'].str.replace(pat=r'[^\w]', repl=r'', regex=True)
        split_2['접수대수_택시'] = split_2['접수대수_택시'].str.replace(pat=r'[^\w]', repl=r'', regex=True)
        split_2['접수대수_우선비대상'] = split_2['접수대수_우선비대상'].str.replace(pat=r'[^\w]', repl=r'', regex=True)


        # 출고 잔여대수 parsing
        split_4 = df.출고잔여대수.str.split(' ')

        # series->dataframe
        split_4 = split_4.apply(lambda x: pd.Series(x))

        # 컬럼명지정
        split_4.columns = ['출고잔여대수_전체', '출고잔여대수_우선순위', '출고잔여대수_법인기관', '출고잔여대수_택시', '출고잔여대수_우선비대상']

        # data type 변경
        split_1 = split_1.astype({'민간공고대수_전체': 'int'})
        split_2 = split_2.astype({'접수대수_우선순위': 'int', '접수대수_법인기관': 'int', '접수대수_택시': 'int',
                                  '접수대수_우선비대상': 'int'})
        split_4 = split_4.astype({'출고잔여대수_전체': 'int'})

        # merge
        result = pd.concat([split_1, split_2, split_4], axis=1)
        # print(result)

        # split_2 접수대수 우선순위, 법인, 택시, 일반 총합계 num_recept_all' 생성
        result['num_recept_all'] = split_2[['접수대수_우선순위', '접수대수_법인기관', '접수대수_택시', '접수대수_우선비대상']].sum(axis=1).values


        # 순서변경
        result = result[['민간공고대수_전체', '민간공고대수_우선순위', '민간공고대수_법인기관', '민간공고대수_택시', '민간공고대수_우선비대상',
             'num_recept_all', '접수대수_우선순위', '접수대수_법인기관', '접수대수_택시', '접수대수_우선비대상', '출고잔여대수_전체', '출고잔여대수_우선순위', '출고잔여대수_법인기관', '출고잔여대수_택시', '출고잔여대수_우선비대상']]

        # 이름변경
        result.columns = ['num_notice_all', 'num_notice_priority', 'num_notice_corp',
                          'num_notice_taxi', 'num_notice_normal', 'num_recept_all', 'num_recept_priority',
                          'num_recept_corp','num_recept_taxi', 'num_recept_normal', 'num_remains_all',
                          'num_remains_priority', 'num_remains_corp', 'num_remains_taxi', 'num_remains_normal']

        # 전체 접수율 계산
        def get_acceptance_rate(x, y):
            try:#x:접수대수 y:공고대수
                if x==0 and y==0:
                    return 100
                elif x>0 and y==0:
                    return 100
                else:
                    return x/y
            except ZeroDivisionError:
                print('0으로 나눌 수 없음')

        result['acceptance_rate_all'] = result.apply(lambda x: get_acceptance_rate(x['num_recept_all'], x['num_notice_all']), axis=1)

        # 전체 접수율 퍼센티지
        def change_percent(v):
            if v == 100:
                return 100
            else:
                return round(v, 2)*100#소수점 둘째자리까지 반올림, 100곱하기

        result['acceptance_rate_all'] = result.apply(lambda v: change_percent(v['acceptance_rate_all']), axis=1)

        self.result = result
        # print(result)


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
        # 2022 지자체 자종별 보조금 table 갖고 오기
        table2 = parser_functions.make2d(data)
        # # print(table[0])
        # # print(table[1:])
        self.table2 = table2

        # parsing
        # 모든 열 출력
        pd.set_option('display.max_columns', None)
        # check_subsidy_area 데이터프레임 생성
        df_subsidy = pd.DataFrame(data=self.table2[1:], columns=self.table2[0])
        # print(df)
        self.df_subsidy = df_subsidy


        # 모델별 지방비 조회
        # def sido_model_insert(optionNo, sidoNo):
        #     # 새창의 모델별 지방비 조회버튼값
        #     option_btn = driver.find_element_by_xpath('/ html / body / form / table / tbody / tr['+ optionNo +'] / td[1] / a')
        #     option_btn.click()#조회 버튼 클릭
        #
        #     # 모델별 지방비 table 갖고 오기
        #     html2 = driver.page_source
        #     soup2 = bs(html2, 'html.parser')
        #     data2 = soup2.find('table', {'class': 'table_02_2_1'})
        #     model_table = parser_functions.make2d(data2)
        #     self.model_table = model_table
        #
        #     # 모든 열 출력
        #     pd.set_option('display.max_columns', None)
        #
        #     # parsing
        #     df_optionNo = pd.DataFrame(data=self.model_table[1:], columns=self.model_table[sidoNo])#0~160
        #     # self.df_optionNo = df_optionNo
        #     # print(df_optionNo)
        #
        #     check_subsidy_area_df = df_optionNo[['제조사', '모델명', '보조금(만원)' ]]
        #     check_subsidy_area_df['sido'] = self.df_subsidy['시도'][sidoNo]#0~160
        #     check_subsidy_area_df['region'] = self.df_subsidy['지역구분'][sidoNo]#0~160
        #     check_subsidy_area_df['car_price'] = 5000
        #
        #     # type 변경
        #     check_subsidy_area_df = check_subsidy_area_df.astype({'보조금(만원)': 'int'})
        #     check_subsidy_area_df['sum_price'] = check_subsidy_area_df['car_price'] - check_subsidy_area_df['보조금(만원)']
        #
        #     # 데이터프레임 특수문자 제거
        #     check_subsidy_area_df['subsidy'] = df_optionNo['보조금(만원)'].str.replace(pat=r'[^\w]', repl=r'',regex=True)
        #
        #     # 민간공고대수 전체, 잔여대수 전체, 접수율 전체
        #     check_subsidy_area_df['num_notice_all'] = self.result['num_notice_all'][sidoNo]#0~160
        #     check_subsidy_area_df['num_remains_all'] = self.result['num_remains_all'][sidoNo]#0~160
        #     check_subsidy_area_df['acceptance_rate_all'] = self.result['acceptance_rate_all'][sidoNo]#0~160
        #
        #     # row 생성 일자
        #     check_subsidy_area_df['created_at'] = datetime.datetime.now()
        #     check_subsidy_area_df['updated_at'] = datetime.datetime.now()
        #
        #     # id 추가
        #     pre_df = pd.read_sql('select*from ev_subsidy_calculator', db.connect)
        #     # print(len(pre_df['id']))
        #
        #     pre_num = len(pre_df['id'])
        #     id = list(range(pre_num, pre_num + 61, 1))
        #     # 9822(61x161)
        #
        #     # 0번째 칼럼에 id 리스트 추가
        #     check_subsidy_area_df.insert(0, 'id', id, True)
        #
        #
        #     # 순서 변경
        #     check_subsidy_area_df = check_subsidy_area_df[['id','sido','region','제조사','모델명', 'subsidy', 'num_notice_all', 'num_remains_all', 'acceptance_rate_all', 'car_price', 'sum_price', 'created_at', 'updated_at']]
        #     # 이름 변경
        #     check_subsidy_area_df.columns = ['id','sido','region','importer','model', 'subsidy', 'num_notice_all', 'num_remains_all', 'acceptance_rate_all', 'car_price', 'sum_price', 'created_at', 'updated_at']
        #     print(check_subsidy_area_df)
        #
        #     # data insert
        #     try:
        #         check_subsidy_area_df.to_sql(name='ev_subsidy_calculator', con=db.engine, if_exists='append',
        #                                        index=False)  # table이 있는 경우 if_exists='append' 사용, 값을 변경하려면 replace, index는 생성하지 말기
        #         print('ev_subsidy_calculator insert 완료')
        #     except Exception as e:
        #         print(e)
        #
        #     # 뒤로가기 버튼 클릭
        #     back_btn = driver.find_element_by_xpath('/ html / body / form / div / a')
        #     back_btn.click()

        # 반복문으로 전체 조회 버튼 조회
        option_list = list(range(1,4))#optionNo은 1~161번까지 (1, 162), #sido_num은 0에서 160까지 (0, 160)
        for i in option_list:
            print(i[1:162])
            print(i[0:161])
            # sido_model_insert(str(i))


    # 문제 마지막 self.df_optionNo만 나옴, sido_car_option함수 내에서 parsing 후 db에 데이터 넣고, back 버튼을 누르고 다시 불러온 데이터를 넣고 반복해야 할 듯


# crawler 실행
SI = subsidy_info_insert('https://ev.or.kr/portal/localInfo', req)
SI.crawler_notice()
SI.crawler_notice_parsing()
SI.crawler_subsidy()


# table insert(1번만 실행해도 됨)
# SI.subsidy_calculator_insert()


