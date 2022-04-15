# -*- coding: utf-8 -*-
# 한글 인코딩 에러
import requests as req
from bs4 import BeautifulSoup as bs
from html_table_parser import parser_functions#호출한 data 표형식으로 보기

from db import db #SQLALchemy를 db변수로 인스턴스한 파일명
from model import table_create #모델이 있는 파일 명

import json
import pandas as pd
import datetime
from collections import Counter
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session



class data_insert():
    def __init__(self, url, req, Session):
        self.req = req
        self.url = url
        self.session = Session()

    def crawler(self):
        req = self.req.get(self.url)
        soup = bs(req.text, 'html.parser')
        data = soup.find('table', {'class':'table_02_2_1'})
        # print(data)
        # table 갖고 오기
        table = parser_functions.make2d(data)
        print(table[1:])
        # print(table[0])
        self.table = table

    # subsidy_info insert 보조금 기본 정보
    def subsidy_info_insert(self):
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

        # print(split_1)

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

        # print(split_2)

        # 출고대수 칼럼 parsing
        split_3 = df.출고대수.str.split(' ')

        # series->dataframe
        split_3 = split_3.apply(lambda x: pd.Series(x))

        # 컬럼명지정
        split_3.columns = ['출고대수_전체', '출고대수_우선순위', '출고대수_법인기관', '출고대수_택시', '출고대수_우선비대상']

        # 괄호값 제거(정규식 특수문자 제거)
        split_3['출고대수_우선순위'] = split_3['출고대수_우선순위'].str.replace(pat=r'[^\w]', repl=r'', regex=True)
        split_3['출고대수_법인기관'] = split_3['출고대수_법인기관'].str.replace(pat=r'[^\w]', repl=r'', regex=True)
        split_3['출고대수_택시'] = split_3['출고대수_택시'].str.replace(pat=r'[^\w]', repl=r'', regex=True)
        split_3['출고대수_우선비대상'] = split_3['출고대수_우선비대상'].str.replace(pat=r'[^\w]', repl=r'', regex=True)

        # print(split_3)

        # 출고잔여대수 칼럼 parsing
        split_4 = df.출고잔여대수.str.split(' ')

        # series->dataframe
        split_4 = split_4.apply(lambda x: pd.Series(x))

        # 컬럼명지정
        split_4.columns = ['출고잔여대수_전체', '출고잔여대수_우선순위', '출고잔여대수_법인기관', '출고잔여대수_택시', '출고잔여대수_우선비대상']

        # 괄호값 제거(정규식 특수문자 제거)
        split_4['출고잔여대수_우선순위'] = split_4['출고잔여대수_우선순위'].str.replace(pat=r'[^\w]', repl=r'', regex=True)
        split_4['출고잔여대수_법인기관'] = split_4['출고잔여대수_법인기관'].str.replace(pat=r'[^\w]', repl=r'', regex=True)
        split_4['출고잔여대수_택시'] = split_4['출고잔여대수_택시'].str.replace(pat=r'[^\w]', repl=r'', regex=True)
        split_4['출고잔여대수_우선비대상'] = split_4['출고잔여대수_우선비대상'].str.replace(pat=r'[^\w]', repl=r'', regex=True)

        # print(split_4)

        # 필요 데이터만 merge(date, sido, region, split_1, split_2, split_3, split_4, note)

        # 일자 생성
        now = datetime.datetime.now()  # 지금시간
        nowToday = now.strftime('%Y%m%d')  # 일자
        # print(nowToday)
        # print(type(split_1))

        # merge
        result = pd.concat([split_1, split_2, split_3, split_4], axis=1)
        # print(result)

        # # date칼럼 생성
        result['date'] = nowToday
        result['sido'] = df['시도']
        result['region'] = df['지역구분']
        result['note'] = df['비고']

        # 순서변경
        result = result[
            ['date', 'sido', 'region', '민간공고대수_전체', '민간공고대수_우선순위', '민간공고대수_법인기관', '민간공고대수_택시', '민간공고대수_우선비대상',
             '접수대수_우선순위', '접수대수_법인기관', '접수대수_택시', '접수대수_우선비대상', '출고대수_전체', '출고대수_우선순위', '출고대수_법인기관', '출고대수_택시',
             '출고대수_우선비대상', '출고잔여대수_전체', '출고잔여대수_우선순위', '출고잔여대수_법인기관', '출고잔여대수_택시', '출고잔여대수_우선비대상']]

        # 이름변경
        result.columns = ['date', 'sido', 'region', 'num_notice_all', 'num_notice_priority', 'num_notice_corp',
                          'num_notice_taxi', 'num_notice_normal', 'num_recept_priority', 'num_recept_corp',
                          'num_recept_taxi', 'num_recept_normal', 'num_release_all', 'num_release_priority',
                          'num_release_corp', 'num_release_taxi', 'num_release_normal', 'num_remains_all',
                          'num_remains_priority', 'num_remains_corp', 'num_remains_taxi', 'num_remains_normal']
        print(result)

        # try:
        #     date = result['date']
        #     sido = result['sido']
        #     region = result['region']
        #     num_notice_all = result['num_notice_all']
        #     num_notice_priority = result['num_notice_priority']
        #     num_notice_corp = result['num_notice_corp']
        #     num_notice_taxi = result['num_notice_taxi']
        #     num_notice_normal = result['num_notice_normal']
        #     num_recept_all = #새로 data 생성해야함
        #     num_recept_priority = result['num_recept_priority']
        #     num_recept_corp = result['num_recept_corp']
        #     num_recept_taxi = result['num_recept_taxi']
        #     num_recept_normal = result['num_recept_normal']
        #     num_release_all = result['num_release_all']
        #     num_release_priority = result['num_release_priority']
        #     num_release_corp = result['num_release_corp']
        #     num_release_taxi = result['num_release_taxi']
        #     num_release_normal = result['num_release_normal']
        #     num_remains_all = result['num_release_all']
        #     num_remains_priority = result['num_release_priority']
        #     num_remains_corp = result['num_remains_corp']
        #     num_remains_taxi = result['num_remains_taxi']
        #     num_remains_normal = result['num_remains_normal']
        #
        #     self.session.excute(f"""
        #                     INSERT INTO customer_service_record (date, sido, region, num_notice_all, num_notice_priority, num_notice_corp, num_notice_taxi, num_notice_normal,num_recept_all, num_recept_priority, num_recept_corp,num_recept_taxi, num_recept_normal, num_release_all, num_release_priority, num_release_corp, num_release_taxi, num_release_normal, num_remains_all,
        #                                                         num_remains_priority, num_remains_corp, num_remains_taxi, num_remains_normal, created_at, updated_at)
        #                     VALUES ("{date}", "{sido}", "{region}", "{num_notice_all}", {num_notice_priority}, {num_notice_corp}, "{num_notice_taxi}","{num_notice_normal}","{num_recept_all}", "{num_recept_priority}","{num_recept_corp}","{num_recept_taxi}","{num_recept_normal}","{num_release_all}","{num_release_priority}","{num_release_corp}","{num_release_taxi}","{num_release_normal}","{num_remains_all}","{num_remains_priority}","{num_remains_corp}","{num_remains_taxi}","{num_remains_normal}","{datetime.now()}", "{datetime.now()}")
        #                     """)
        #     self.session.commit()
        #
        # except Exception as e:
        #     print(e)

        # db insert
        # id
        # sido
        # region
        # num_notice_all
        # num_notice_priority
        # num_notice_corp
        # num_notice_taxi
        # num_notice_normal
        # num_recept_all
        # num_recept_priority
        # num_recept_corp
        # num_recept_taxi
        # num_recept_normal
        # num_release_all
        # num_release_priority
        # num_release_corp
        # num_release_taxi
        # num_release_normal
        # num_remains_all
        # num_remains_priority
        # num_remains_corp
        # num_remains_taxi
        # num_remains_normal
        # created_at
        # updated_at
    # subsidy_accepted insert 보조금 접수 가능


    # subsidy_trend insert 보조금 트렌드


    # subsidy_closing _area 보조금 마감지역


    # check_subsidy_area 지역 보조금 확인

# crawler 실행

DI = data_insert('https://ev.or.kr/portal/localInfo', req, Session)
DI.crawler()
DI.subsidy_info_insert()