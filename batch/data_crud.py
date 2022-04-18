# -*- coding: utf-8 -*-
# 한글 인코딩 에러
import requests as req
import sqlalchemy
from bs4 import BeautifulSoup as bs
from html_table_parser import parser_functions#호출한 data 표형식으로 보기

from db import db #SQLALchemy를 db변수로 인스턴스한 파일명
from model import models #모델이 있는 파일 명
import json

import pandas as pd
import datetime

from model import models
from sqlalchemy.orm import sessionmaker, scoped_session, session

# DB선언
from sqlalchemy import create_engine, Table, MetaData

# db 연결
SQLALCHEMY_DATABASE_URI = 'mysql+pymysql://admin:worksdev77!@bmwgs-2.cj9bzkwqopkp.ap-northeast-2.rds.amazonaws.com:3306/evsadb'
engine = sqlalchemy.create_engine(SQLALCHEMY_DATABASE_URI, echo=False)
connect = engine.connect()
meta = MetaData()
Session = sessionmaker(bind=engine)
session = Session()


class data_insert():
    def __init__(self, url, req):
        self.req = req
        self.url = url

    def crawler(self):
        req = self.req.get(self.url)
        soup = bs(req.text, 'html.parser')
        data = soup.find('table', {'class':'table_02_2_1'})
        # print(data)
        # table 갖고 오기
        table = parser_functions.make2d(data)
        # print(table[1:])
        # print(table[0])
        self.table = table

    def crawler_parsing(self):
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

        # date칼럼 생성
        result['date'] = nowToday
        result['sido'] = df['시도']
        result['region'] = df['지역구분']
        result['note'] = df['비고']
        result['created_at'] = datetime.datetime.now()
        result['updated_at'] = datetime.datetime.now()

        # data type 변경
        split_2 = split_2.astype({'접수대수_우선순위': 'int', '접수대수_법인기관': 'int', '접수대수_택시': 'int',
                       '접수대수_우선비대상': 'int'})

        # split_2 접수대수 우선순위, 법인, 택시, 일반 총합계 num_recept_all' 생성
        result['num_recept_all'] = split_2[['접수대수_우선순위', '접수대수_법인기관', '접수대수_택시', '접수대수_우선비대상']].sum(axis=1).values

        self.update_num_recept_all = result['num_recept_all']

        # 순서변경
        result = result[['date', 'sido', 'region', '민간공고대수_전체', '민간공고대수_우선순위', '민간공고대수_법인기관', '민간공고대수_택시', '민간공고대수_우선비대상',
             'num_recept_all', '접수대수_우선순위', '접수대수_법인기관', '접수대수_택시', '접수대수_우선비대상', '출고대수_전체', '출고대수_우선순위', '출고대수_법인기관', '출고대수_택시',
             '출고대수_우선비대상', '출고잔여대수_전체', '출고잔여대수_우선순위', '출고잔여대수_법인기관', '출고잔여대수_택시', '출고잔여대수_우선비대상', 'note', 'created_at', 'updated_at']]

        # 이름변경
        result.columns = ['date', 'sido', 'region', 'num_notice_all', 'num_notice_priority', 'num_notice_corp',
                          'num_notice_taxi', 'num_notice_normal', 'num_recept_all', 'num_recept_priority', 'num_recept_corp',
                          'num_recept_taxi', 'num_recept_normal', 'num_release_all', 'num_release_priority',
                          'num_release_corp', 'num_release_taxi', 'num_release_normal', 'num_remains_all',
                          'num_remains_priority', 'num_remains_corp', 'num_remains_taxi', 'num_remains_normal', 'note', 'created_at', 'updated_at']
        # print(result)

        self.result = result

    # subsidy_info insert 보조금 기본 정보
    def subsidy_info_insert(self):
        try:
            # subsidy_info table insert
            self.result.to_sql(name='subsidy_info',con=engine,if_exists='append',index=True)#table이 있는 경우 if_exists='append' 사용, 값을 변경하려면 replace
            print('subsidy_info insert 완료')
        except Exception as e:
            print(e)

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
    def subsidy_accepted(self):
        # 잔여대수 전체, 잔여대수 우선순위, 잔여대수 법인, 잔여대수 택시, 잔여대수 일반
        subsidy_accepted_df = self.result[['num_remains_all','num_remains_priority','num_remains_corp', 'num_remains_taxi', 'num_remains_normal']]

        # data type 변경
        type_change_df = self.result.astype({'num_recept_all':'int', 'num_notice_all':'int', 'num_notice_priority':'int','num_recept_priority':'int', 'num_notice_corp':'int','num_recept_corp':'int', 'num_notice_taxi':'int','num_recept_taxi':'int', 'num_notice_normal':'int','num_recept_normal':'int'})

        # type 확인
        print(type_change_df.dtypes)
        print(type_change_df['num_recept_all'])

        # 접수율 = 전체 접수대수 / 전체 공고대수
        # 접수율 전체, 접수율 우선순위, 접수율 법인, 접수율 택시, 접수율 일반, 접수 가능여부
        # subsidy_accepted_df['acceptance_rate_all'] = self.result['num_recept_all'] / self.result['num_notice_all']
        # print(subsidy_accepted_df)


    # subsidy_trend insert 보조금 트렌드

    #
    #
    # subsidy_closing _area 보조금 마감지역
    #
    #
    # check_subsidy_area 지역 보조금 확인
    #

    # update
    def update_table_multiply(self):
        # model.models오브젝트의 table에 쿼리 날리기
        # print(self.update_num_recept_all.values)

        # 대량 값 update 하기
        list = self.update_num_recept_all.values
        # 1~161 인덱스로 dict 자료형 만들기
        dict = {(i+1): list[i] for i in range(0, len(list))}
        print(dict)

        for key, value in dict.items():
            query = session.query(models.subsidy_info).filter(models.subsidy_info.id==key)
            query.update({models.subsidy_info.num_recept_all:value})
            session.commit()
        print('update 완료')


# crawler 실행
DI = data_insert('https://ev.or.kr/portal/localInfo', req)
DI.crawler()
DI.crawler_parsing()
# DI.subsidy_info_insert()
DI.subsidy_accepted()
# DI.update_table_multiply()