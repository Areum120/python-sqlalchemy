# -*- coding: utf-8 -*-
# 한글 인코딩 에러
import requests as req
import sqlalchemy
from bs4 import BeautifulSoup as bs
from html_table_parser import parser_functions#호출한 data 표형식으로 보기

# 경고창 무시
import warnings
warnings.filterwarnings('ignore')

# parsing
import pandas as pd
import datetime
import math

# object 연결
from model import models

# db session 연결
import db


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

        # id칼럼 생성
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

        # type변경
        result = result.astype({'num_notice_all':'int', 'num_notice_priority':'int', 'num_notice_corp':'int',
                          'num_notice_taxi':'int', 'num_notice_normal':'int', 'num_recept_all':'int', 'num_recept_priority':'int', 'num_recept_corp':'int',
                          'num_recept_taxi':'int', 'num_recept_normal':'int', 'num_release_all':'int', 'num_release_priority':'int',
                          'num_release_corp':'int', 'num_release_taxi':'int', 'num_release_normal':'int', 'num_remains_all':'int',
                          'num_remains_priority':'int', 'num_remains_corp':'int', 'num_remains_taxi':'int', 'num_remains_normal':'int'})

        self.result = result

    # subsidy_info insert 보조금 기본 정보
    def subsidy_info_insert(self):
        # subsidy_info table insert

        # id값 생성
        # db에 있는 전날 subsidy_info data 불러오기
        pre_df = pd.read_sql('select*from subsidy_info', db.connect)
        print(len(pre_df['id']))

        pre_num = len(pre_df['id'])
        id = list(range(pre_num, pre_num+161, 1))
        print(id)
        print(len(id))

        # 0번재 칼럼에 id 리스트 추가
        self.result.insert(0, 'id', id, True)
        print(self.result)

        try:
            self.result.to_sql(name='subsidy_info',con=db.engine,if_exists='append',index=False)#table이 있는 경우 if_exists='append' 사용, 값을 변경하려면 replace
            print('subsidy_info insert 완료')
        except Exception as e:
            print(e)

    # subsidy_accepted insert 보조금 접수 가능
    def subsidy_accepted(self):
        # 잔여대수 전체, 잔여대수 우선순위, 잔여대수 법인, 잔여대수 택시, 잔여대수 일반
        subsidy_accepted_df = self.result[['date', 'num_notice_all', 'num_notice_priority',  'num_notice_corp',  'num_recept_all', 'num_notice_taxi', 'num_notice_normal', 'num_recept_all', 'num_recept_priority', 'num_recept_corp', 'num_recept_taxi', 'num_recept_normal', 'num_remains_all','num_remains_priority','num_remains_corp', 'num_remains_taxi', 'num_remains_normal']]

        # 접수율 = 전체 접수대수 / 전체 공고대수
        # 접수율 전체, 접수율 우선순위, 접수율 법인, 접수율 택시, 접수율 일반, 접수 가능여부

        # 예외사항 - 0나누기 0은 nan으로 나오거나 3으로 0을 나눌 수 없어서 inf가 나옴. 만약 공고수가 0이고 접수대수가 0이상이면 접수율 100%로 변환하여 접수 불가능이라고 뜨게 하기
        # lambda 함수 사용, axis=1 꼭 해주기

        def get_acceptance_rate(x, y):
            try:
                if x==0 and y==0:
                    return 100
                elif x>0 and y==0:
                    return 100
                else:
                    return x/y
            except ZeroDivisionError:
                print('0으로 나눌 수 없음')

        subsidy_accepted_df['acceptance_rate_all'] = subsidy_accepted_df.apply(lambda x: get_acceptance_rate(x['num_recept_all'], x['num_notice_all']), axis=1)
        subsidy_accepted_df['acceptance_rate_priority'] = subsidy_accepted_df.apply(lambda x: get_acceptance_rate(x['num_recept_priority'], x['num_notice_priority']), axis=1)
        subsidy_accepted_df['acceptance_rate_corp'] = subsidy_accepted_df.apply(lambda x: get_acceptance_rate(x['num_recept_corp'], x['num_notice_corp']), axis=1)
        subsidy_accepted_df['acceptance_rate_taxi'] = subsidy_accepted_df.apply(lambda x: get_acceptance_rate(x['num_recept_taxi'], x['num_notice_taxi']), axis=1)
        subsidy_accepted_df['acceptance_rate_normal'] = subsidy_accepted_df.apply(lambda x: get_acceptance_rate(x['num_recept_normal'], x['num_notice_normal']), axis=1)

        # apply lambda if문 다중조건, inline 절로 표현 2
        # type_change_df['acceptance_rate_all'] = type_change_df.apply(lambda x: '100' if (x['num_notice_all'] == 0 and x['num_recept_all'] ==0) else (x['num_recept_all'] / x['num_notice_all'])('100' if (x['num_notice_all'] == 0 and x['num_recept_all'] > 0) else (x['num_recept_all'] / x['num_notice_all'])), axis=1)
        # print(type_change_df)

        # 퍼센티지 바꾸기
        def change_percent(v):
            if v == 100:
                return 100
            else:
                return round(v, 2)*100#2자리수부터 반올림, 100곱하기

        subsidy_accepted_df['acceptance_rate_all'] = subsidy_accepted_df.apply(lambda v: change_percent(v['acceptance_rate_all']), axis=1)
        subsidy_accepted_df['acceptance_rate_priority'] = subsidy_accepted_df.apply(lambda v: change_percent(v['acceptance_rate_priority']), axis=1)
        subsidy_accepted_df['acceptance_rate_corp'] = subsidy_accepted_df.apply(lambda v: change_percent(v['acceptance_rate_corp']), axis=1)
        subsidy_accepted_df['acceptance_rate_taxi'] = subsidy_accepted_df.apply(lambda v: change_percent(v['acceptance_rate_taxi']), axis=1)
        subsidy_accepted_df['acceptance_rate_normal'] = subsidy_accepted_df.apply(lambda v: change_percent(v['acceptance_rate_normal']), axis=1)

        # 접수 가능여부 칼럼 생성
        def get_availability(z):
            if z < 100:
                return True
            else:
                return False

        subsidy_accepted_df['availability_all'] = subsidy_accepted_df.apply(lambda z: get_availability(z['acceptance_rate_all']), axis=1)
        subsidy_accepted_df['availability_priority'] = subsidy_accepted_df.apply(lambda z: get_availability(z['acceptance_rate_priority']), axis=1)
        subsidy_accepted_df['availability_corp'] = subsidy_accepted_df.apply(lambda z: get_availability(z['acceptance_rate_corp']), axis=1)
        subsidy_accepted_df['availability_taxi'] = subsidy_accepted_df.apply(lambda z: get_availability(z['acceptance_rate_taxi']), axis=1)
        subsidy_accepted_df['availability_normal'] = subsidy_accepted_df.apply(lambda z: get_availability(z['acceptance_rate_normal']), axis=1)

        # row 생성 일자
        subsidy_accepted_df['created_at'] = datetime.datetime.now()
        subsidy_accepted_df['updated_at'] = datetime.datetime.now()

        print(subsidy_accepted_df)

        # subsidy_accepted table insert
        try:
            subsidy_accepted_df.to_sql(name='subsidy_accepted',con=db.engine,if_exists='append',index=False)#table이 있는 경우 if_exists='append' 사용, 값을 변경하려면 replace
            print('subsidy_accepted insert 완료')
        except Exception as e:
            print(e)

    # subsidy_trend insert 보조금 트렌드
    def subsidy_trend(self):
        subsidy_trend_df =  self.result[['date','num_notice_all', 'num_notice_priority', 'num_notice_corp',
                          'num_notice_taxi', 'num_notice_normal', 'num_recept_all', 'num_recept_priority', 'num_recept_corp',
                          'num_recept_taxi', 'num_recept_normal', 'num_release_all', 'num_release_priority',
                          'num_release_corp', 'num_release_taxi', 'num_release_normal']]

        # 일별 접수 대수 칼럼 생성
        # 어제 날짜 구하기
        today = datetime.date.today()
        yesterday = today - datetime.timedelta(1)
        yesterday = str(yesterday)

        # db에 있는 전날 subsidy_info data 불러오기
        yesterday_df = pd.read_sql('select*from subsidy_info', db.connect)

        # yesterday data type 변환
        yesterday_df = yesterday_df.astype({'date':'str','num_recept_all':'int','num_recept_priority':'int','num_recept_corp':'int','num_recept_taxi':'int','num_recept_normal':'int'})

        # 전일 df 추리기
        yesterday_df = yesterday_df[yesterday_df['date'] == yesterday]

        # 일별 접수 대수 = 전일 접수대수 - 오늘 접수대수
        subsidy_trend_df['num_daily_recept_all'] = subsidy_trend_df['num_recept_all'] - yesterday_df['num_recept_all']
        subsidy_trend_df['num_daily_recept_priority'] = subsidy_trend_df['num_recept_priority'] - yesterday_df['num_recept_priority']
        subsidy_trend_df['num_daily_recept_corp'] =  subsidy_trend_df['num_recept_corp'] - yesterday_df['num_recept_corp']
        subsidy_trend_df['num_daily_recept_taxi'] = subsidy_trend_df['num_recept_taxi'] - yesterday_df['num_recept_taxi']
        subsidy_trend_df['num_daily_recept_normal'] = subsidy_trend_df['num_recept_normal'] - yesterday_df['num_recept_normal']

        # row 생성 일자
        subsidy_trend_df['created_at'] = datetime.datetime.now()
        subsidy_trend_df['updated_at'] = datetime.datetime.now()

        print(subsidy_trend_df)

        # subsidy_accepted table insert
        try:
            subsidy_trend_df.to_sql(name='subsidy_trend', con=db.engine, if_exists='append',
                                       index=False)  # table이 있는 경우 if_exists='append' 사용, 값을 변경하려면 replace
            print('subsidy_trend insert 완료')
        except Exception as e:
            print(e)

    # subsidy_closing _area 보조금 마감지역



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
            query = db.session.query(models.subsidy_info).filter(models.subsidy_info.id==key)
            query.update({models.subsidy_info.num_recept_all:value})
            db.session.commit()
        print('update 완료')

    # upgrade example
    # def upgrade():
    #     ### commands auto generated by Alembic - please adjust! ###
    #     op.add_column('contract_type', sa.Column('allow_opportunities', sa.Boolean(), nullable=True))
    #     op.add_column('contract_type', sa.Column('opportunity_response_instructions', sa.Text(), nullable=True))
    #     op.create_foreign_key('created_by_id_fkey', 'job_status', 'users', ['created_by_id'], ['id'])
    #     op.create_foreign_key('updated_by_id_fkey', 'job_status', 'users', ['updated_by_id'], ['id'])
    #     op.add_column('opportunity', sa.Column('opportunity_type_id', sa.Integer(), nullable=True))
    #     op.create_foreign_key(
    #         'opportunity_type_id_contract_type_id_fkey', 'opportunity', 'contract_type',
    #         ['opportunity_type_id'], ['id'])


# crawler 실행
DI = data_insert('https://ev.or.kr/portal/localInfo', req)
DI.crawler()
DI.crawler_parsing()

# insert
# DI.subsidy_info_insert()
DI.subsidy_accepted()
# DI.subsidy_trend()

# update
# DI.update_table_multiply()