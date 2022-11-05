# About SQLAlchemy
`python` 의 `sqlalchemy` 라이브러리를 사용하여 \
Database Connection Pool 을 구현한 wrapper 패키지입니다.
- DataSource
  > Database 와 통신할 수 있는 
- SqlSession 
  > DataSource 로부터 session 을 받고 해당 session 객체를 사용하여 Database 와 통신합니다.
# Simple Usage
```python
import atexit
from factory.DataSource import DataSource
from factory.SqlSession import SqlSession

# 1. Database 와의 연결을 위해 DataSource 객체를 생성하고 초기화합니다.
ds = DataSource()
ds.init(
    """postgresql://{user}:{password}@{host}:{port}/{database}""".format(
        user='user',
        password='password',
        host='hostname',
        port=5432,
        database='dbname'
    ),
    pool_size=5,
    max_overflow=0,
)

# 2. 프로세스 종료 시에 Database 와의 연결을 종료하고 DataSource 객체를 제거합니다.
atexit.register(ds.__del__)

# 3. DataSource 로부터 session 객체를 받고 이를 사용하여 Database 와 통신합니다.
#    with 컨텍스트 내에서 사용하도록 설계되었습니다.
with ds.get_session() as session:
    result = session.select_one("""
        select
            1 as col_a
          , 2 as col_b
        from dual
    """)
print('result')
print(result)
```
