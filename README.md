# Python SqlAlchemy 예제

## Library version
* Flask==2.1.1
* Flask-SQLAlchemy==2.5.1

## 사용방법

config.py에 db의 uri를 다음과 같이 입력 합니다.
```python
SQLALCHEMY_DATABASE_URI = 'mysql+pymysql://admin:<pw>@<host_name>:3306/testdb'
```

main.py를 실행 합니다.
