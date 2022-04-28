# -*- coding: utf-8 -*-
# 한글 인코딩 에러
# crawling
import requests as req
from bs4 import BeautifulSoup as bs
from html_table_parser import parser_functions#호출한 data 표형식으로 보기
# 경고창 무시
import warnings
warnings.filterwarnings('ignore')
# parsing
import pandas as pd
import datetime
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

        # 전일 넣은 id 이후부터 생성
        # db에 있는 전날 subsidy_info data 불러오기
        pre_df = pd.read_sql('select*from subsidy_info', db.connect)
        # print(len(pre_df['id']))

        pre_num = len(pre_df['id'])
        id = list(range(pre_num, pre_num+161, 1))
        # print(id)
        # print(len(id))

        # 0번째 칼럼에 id 리스트 추가
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
        result_df = self.result[['date', 'sido', 'region', 'num_notice_all', 'num_notice_priority',  'num_notice_corp',  'num_notice_taxi', 'num_notice_normal', 'num_recept_all', 'num_recept_priority', 'num_recept_corp', 'num_recept_taxi', 'num_recept_normal', 'num_remains_all','num_remains_priority','num_remains_corp', 'num_remains_taxi', 'num_remains_normal']]

        # 접수율 = 전체 접수대수 / 전체 공고대수
        # 접수율 전체, 접수율 우선순위, 접수율 법인, 접수율 택시, 접수율 일반, 접수 가능여부

        # 예외사항 - 0나누기 0은 nan으로 나오거나 3으로 0을 나눌 수 없어서 inf가 나옴. 만약 공고수가 0이고 접수대수가 0이상이면 접수율 100%로 변환하여 접수 불가능이라고 뜨게 하기
        # lambda 함수 사용, axis=1 꼭 해주기

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

        result_df['acceptance_rate_all'] = result_df.apply(lambda x: get_acceptance_rate(x['num_recept_all'], x['num_notice_all']), axis=1)
        result_df['acceptance_rate_priority'] = result_df.apply(lambda x: get_acceptance_rate(x['num_recept_priority'], x['num_notice_priority']), axis=1)
        result_df['acceptance_rate_corp'] = result_df.apply(lambda x: get_acceptance_rate(x['num_recept_corp'], x['num_notice_corp']), axis=1)
        result_df['acceptance_rate_taxi'] = result_df.apply(lambda x: get_acceptance_rate(x['num_recept_taxi'], x['num_notice_taxi']), axis=1)
        result_df['acceptance_rate_normal'] = result_df.apply(lambda x: get_acceptance_rate(x['num_recept_normal'], x['num_notice_normal']), axis=1)

        # apply lambda if문 다중조건, inline 절로 표현 2
        # type_change_df['acceptance_rate_all'] = type_change_df.apply(lambda x: '100' if (x['num_notice_all'] == 0 and x['num_recept_all'] ==0) else (x['num_recept_all'] / x['num_notice_all'])('100' if (x['num_notice_all'] == 0 and x['num_recept_all'] > 0) else (x['num_recept_all'] / x['num_notice_all'])), axis=1)
        # print(type_change_df)


        # 퍼센티지 바꾸기
        def change_percent(v):
            if v == 100:
                return 100
            else:
                return round(v, 2)*100#소수점 둘째자리까지 반올림, 100곱하기

        result_df['acceptance_rate_all'] = result_df.apply(lambda v: change_percent(v['acceptance_rate_all']), axis=1)
        result_df['acceptance_rate_priority'] = result_df.apply(lambda v: change_percent(v['acceptance_rate_priority']), axis=1)
        result_df['acceptance_rate_corp'] = result_df.apply(lambda v: change_percent(v['acceptance_rate_corp']), axis=1)
        result_df['acceptance_rate_taxi'] = result_df.apply(lambda v: change_percent(v['acceptance_rate_taxi']), axis=1)
        result_df['acceptance_rate_normal'] = result_df.apply(lambda v: change_percent(v['acceptance_rate_normal']), axis=1)

        # print(result_df)

        # 161개 지자체 평균 접수율 계산
        acceptance_rate = result_df.mean()
        print(round(acceptance_rate['acceptance_rate_all'],1))
        print(round(acceptance_rate['acceptance_rate_priority'],1))
        print(round(acceptance_rate['acceptance_rate_corp'],1))
        print(round(acceptance_rate['acceptance_rate_taxi'],1))
        print(round(acceptance_rate['acceptance_rate_normal'],1))

        result_df['apt_all_mean'] = round(acceptance_rate['acceptance_rate_all'],1)
        result_df['apt_priority_mean'] = round(acceptance_rate['acceptance_rate_priority'],1)
        result_df['apt_corp_mean'] = round(acceptance_rate['acceptance_rate_corp'],1)
        result_df['apt_taxi_mean'] = round(acceptance_rate['acceptance_rate_taxi'],1)
        result_df['apt_normal_mean'] = round(acceptance_rate['acceptance_rate_normal'],1)

        # 접수율 비교 함수
        # 1: 매우 높은 편 2: 높은편 3: 보통 4: 낮은편 5: 매우 낮은편 6: 불가능(공고대수가 0인 경우)
        def get_acceptance_compare(x, y, z):
            # y=가 0인 경우, 공고대수가 0이고 접수대수가 0인 경우
            if z==0 and y==0:
                return 6
                # 공고대수가 0이 아니고 접수대수가 0인 경우 포함 나머지 경우의 수
            else:
                # x>y
                if x > y:
                    if x - y == 0:
                        return 3
                    elif x - y >=0 and x - y < 10:# x-y<10#보통
                        return 3
                    elif x - y >= 10 and x - y < 25:# x-y<25#높은 편
                        return 2
                    elif x - y >= 25:# x-y>25#매우 높은 편
                        return 1
                # x<y
                elif x < y:
                    if y - x == 0:
                        return 3
                    elif y - x >= 0 and y - x <= 10: #보통
                        return 3
                    elif y - x >= 10 and y - x < 25:
                        return 4 #낮은 편
                    elif y - x >= 25:
                        return 5 #매우 낮은 편
                # x,y가 같은 경우
                elif x==y:
                    return 3

        # 각 지자체 접수율, 각 지자체 접수율 평균, 각 지자체 공고대수
        result_df['availability_all'] = result_df.apply(lambda z: get_acceptance_compare(z['acceptance_rate_all'], z['apt_all_mean'], z['num_notice_all']), axis=1)
        result_df['availability_priority'] = result_df.apply(lambda z: get_acceptance_compare(z['acceptance_rate_priority'], z['apt_priority_mean'], z['num_notice_priority']), axis=1)
        result_df['availability_corp'] = result_df.apply(lambda z: get_acceptance_compare(z['acceptance_rate_corp'], z['apt_corp_mean'], z['num_notice_corp']), axis=1)
        result_df['availability_taxi'] = result_df.apply(lambda z: get_acceptance_compare(z['acceptance_rate_taxi'], z['apt_taxi_mean'], z['num_notice_taxi']), axis=1)
        result_df['availability_normal'] = result_df.apply(lambda z: get_acceptance_compare(z['acceptance_rate_normal'], z['apt_normal_mean'], z['num_notice_normal']), axis=1)

        # row 생성 일자
        result_df['created_at'] = datetime.datetime.now()
        result_df['updated_at'] = datetime.datetime.now()

        print(result_df)

        # 첫번째로 id 추가할 때
        # id = list(range(0, 161, 1))
        # 0번째 칼럼에 id 리스트 추가
        # result_df.insert(0, 'id', id, True)

        # 전일 넣은 id 이후부터 생성
        # db에 있는 전날 subsidy_accepted data 불러오기
        pre_df = pd.read_sql('select*from subsidy_accepted', db.connect)
        # print(len(pre_df['id']))
        #
        pre_num = len(pre_df['id'])
        id = list(range(pre_num, pre_num + 161, 1))

        # table순서 정리
        subsidy_accepted_df = result_df[['date', 'sido', 'region', 'num_remains_all', 'num_remains_priority', 'num_remains_corp', 'num_remains_taxi',
             'num_remains_normal', 'acceptance_rate_all', 'acceptance_rate_priority', 'acceptance_rate_corp',
             'acceptance_rate_taxi', 'acceptance_rate_normal', 'availability_all', 'availability_priority',
             'availability_corp', 'availability_taxi', 'availability_normal', 'created_at', 'updated_at']]

        # 0번째 칼럼에 id 리스트 추가
        subsidy_accepted_df.insert(0, 'id', id, True)

        self.subsidy_accepted_df = subsidy_accepted_df
        print(subsidy_accepted_df)

        # subsidy_accepted table insert
        try:
            subsidy_accepted_df.to_sql(name='subsidy_accepted',con=db.engine,if_exists='append',index=False)#table이 있는 경우 if_exists='append' 사용, 값을 변경하려면 replace
            print('subsidy_accepted insert 완료')
        except Exception as e:
            print(e)

    # subsidy_trend insert 보조금 트렌드
    def subsidy_trend(self):
        # 4월 19~20 시도, 지역 칼럼 insert
        # sido_df = self.result[['sido', 'region']]
        # yesterday_df = pd.read_sql('select*from subsidy_info', db.connect)
        # sido_df2 = yesterday_df[['sido', 'region']]
       subsidy_trend_df =  self.result[['date', 'sido', 'region', 'num_notice_all', 'num_notice_priority', 'num_notice_corp',
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
       yesterday_df = yesterday_df.reset_index(drop=True)#인덱스 재정렬(기존 인덱스 삭제)

       subsidy_trend_df['num_daily_recept_all'] = subsidy_trend_df['num_recept_all'] - yesterday_df['num_recept_all']
       subsidy_trend_df['num_daily_recept_priority'] = subsidy_trend_df['num_recept_priority'] - yesterday_df['num_recept_priority']
       subsidy_trend_df['num_daily_recept_corp'] =  subsidy_trend_df['num_recept_corp'] - yesterday_df['num_recept_corp']
       subsidy_trend_df['num_daily_recept_taxi'] = subsidy_trend_df['num_recept_taxi'] - yesterday_df['num_recept_taxi']
       subsidy_trend_df['num_daily_recept_normal'] = subsidy_trend_df['num_recept_normal'] - yesterday_df['num_recept_normal']

    # row 생성 일자
       subsidy_trend_df['created_at'] = datetime.datetime.now()
       subsidy_trend_df['updated_at'] = datetime.datetime.now()

       print(subsidy_trend_df)

        # 첫번째로 id 추가할 때
        # id = list(range(0, 161, 1))
        # print(id)
        # print(len(id))

        # 0번째 칼럼에 id 리스트 추가
        # subsidy_trend_df.insert(0, 'id', id, True)
        # print(subsidy_trend_df)

        # 전일 넣은 id 이후부터 생성
        # db에 있는 전날 subsidy_trend data 불러오기
       pre_df = pd.read_sql('select*from subsidy_trend', db.connect)
       # print(len(pre_df['id']))

       pre_num = len(pre_df['id'])
       id = list(range(pre_num, pre_num+161, 1))

        # 0번째 칼럼에 id 리스트 추가
       subsidy_trend_df.insert(0, 'id', id, True)
       print(subsidy_trend_df)

        # subsidy_trend table insert
       try:
          subsidy_trend_df.to_sql(name='subsidy_trend', con=db.engine, if_exists='append',
                                       index=False)  # table이 있는 경우 if_exists='append' 사용, 값을 변경하려면 replace
          print('subsidy_trend insert 완료')
       except Exception as e:
          print(e)

    # subsidy_closing_area 보조금 마감지역
    def subsidy_closing_area(self):
        # 보조금 접수 가능 table의 data 불러오기, DI.subsidy_accepted()를 동시 호출해야 하는 문제가 있음
        subsidy_closing_area_df = self.subsidy_accepted_df[['date', 'sido', 'region', 'acceptance_rate_all','acceptance_rate_corp','acceptance_rate_priority','acceptance_rate_taxi','acceptance_rate_normal']]


        # 보조금,  민간공고대수 법인기관, 접수대수 법인, 캐피탈사(나중에 추가) 칼럼 추가

        # 접수율 마감 여부 data 생성
        def is_deadline(x):
            if x >= 100:
                return 1
            elif x >= 90:
                return 2
            elif x < 90:
                return 3

        subsidy_closing_area_df['is_all_deadline'] = subsidy_closing_area_df.apply(lambda x: is_deadline(x['acceptance_rate_all']), axis=1)
        subsidy_closing_area_df['is_priority_deadline'] = subsidy_closing_area_df.apply(lambda x: is_deadline(x['acceptance_rate_priority']), axis=1)
        subsidy_closing_area_df['is_corp_deadline'] = subsidy_closing_area_df.apply(lambda x: is_deadline(x['acceptance_rate_corp']), axis=1)
        subsidy_closing_area_df['is_taxi_deadline'] = subsidy_closing_area_df.apply(lambda x: is_deadline(x['acceptance_rate_taxi']), axis=1)
        subsidy_closing_area_df['is_normal_deadline'] = subsidy_closing_area_df.apply(lambda x: is_deadline(x['acceptance_rate_normal']), axis=1)

        # row 생성 일자
        subsidy_closing_area_df['created_at'] = datetime.datetime.now()
        subsidy_closing_area_df['updated_at'] = datetime.datetime.now()

        # 첫번째로 id 추가할 때
        # id = list(range(0, 161, 1))

        # 0번째 칼럼에 id 리스트 추가
        # subsidy_closing_area_df.insert(0, 'id', id, True)
        # print(subsidy_closing_area_df)

        # 전일 넣은 id 이후부터 생성
        # db에 있는 전날 subsidy_trend data 불러오기
        pre_df = pd.read_sql('select*from subsidy_accepted', db.connect)
        # print(len(pre_df['id']))

        pre_num = len(pre_df['id'])
        id = list(range(pre_num, pre_num + 161, 1))

        # pre_num = len(pre_df['id'])
        id = list(range(pre_num, pre_num + 161, 1))

        # 0번째 칼럼에 id 리스트 추가
        subsidy_closing_area_df.insert(0, 'id', id, True)
        print(subsidy_closing_area_df)

        # subsidy_closing_area table insert
        try:
            subsidy_closing_area_df.to_sql(name='subsidy_closing_area', con=db.engine, if_exists='append',
                                       index=False)  # table이 있는 경우 if_exists='append' 사용, 값을 변경하려면 replace, index는 생성하지 말기
            print('subsidy_closing_area insert 완료')
        except Exception as e:
            print(e)




# crawler 실행
DI = data_insert('https://ev.or.kr/portal/localInfo', req)
DI.crawler()
DI.crawler_parsing()

# insert
DI.subsidy_info_insert()
DI.subsidy_accepted()
DI.subsidy_trend()
DI.subsidy_closing_area()

# update
# DI.update_table_multiply()