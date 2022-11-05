# -*- coding: utf-8 -*-
import atexit
from typing import Callable

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.pool import Pool

from common.sqlalchemist.factory.SqlSession import SqlSession
from common.sqlalchemist.exceptions.DataSourceError import DataSourceError


def requires_init(func: Callable) -> Callable:
    """
    이 데코레이터가 적용된 메서드는 실행에 앞서 초기화 성공 여부를 검사합니다.
    DataSource 이외의 다른 클래스의 메서드에 적용하면 안 됩니다.
    :param func: 적용 대상 메서드
    :return: wrapped function
    """

    def wrapper(*args, **kwargs):
        # DataSource 이외의 다른 클래스의 메서드에 적용하면 안 됩니다.
        if not args:
            raise TypeError("this decorator should be applied to a instance method of DataSource class.")

        # DataSource 이외의 다른 클래스의 메서드에 적용하면 안 됩니다.
        self: DataSource = args[0]
        if not isinstance(self, DataSource):
            raise TypeError("this decorator should be applied to a instance method of DataSource class.")

        # 현재 Connection Pool 의 가용성 체크
        self.check_initialization()

        return func(*args, **kwargs)

    return wrapper


class DataSource:
    """
    Data Source 클래스 : Database Connection Pool 을 관리
    Pool 로부터 연결을 취득하거나
    더 이상 사용할 일이 없는 연결을 Pool 로 반환하는 역할
    """

    # sqlalchemy engine 에 대한 참조
    _engine: Engine = None

    # Session Pool 에 대한 참조
    _pool: Pool = None

    def __init__(self):
        """
        생성자 : 필요한 환경 구축 작업 수행
        """

    def __del__(self):
        """
        소멸자 : 참조가 수거되는 시점에 호출됩니다.
        혹여 DataSource 닫는 처리를 잊어버리더라도 빼먹지 않기 위함입니다.
        :return:
        """
        try:
            self.close()
            print(f'del {self.__class__.__name__}')
        except (Exception, BaseException) as e:
            raise e

    def __enter__(self):
        """
        with 문 (context manager) 지원을 위한 정의
        :return: self
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        컨텍스트(with 문 등)를 벗어나는 시점에 호출됩니다.
        혹여 DataSource 닫는 처리를 잊어버리더라도 빼먹지 않기 위함입니다.
        :param exc_type:
        :param exc_val:
        :param exc_tb:
        :return:
        """
        try:
            self.close()
            print(f'exit {self.__class__.__name__}')
        except (Exception, BaseException) as e:
            raise e

    def init(self, *args, **kwargs):
        """
        Database Connection Pool 초기화
        :param args: - Application Configuration // type: tuple
        :param kwargs: - Application Configuration // type: dict
        :return: void
        """

        # sqlalchemy engine 생성
        kwargs.update({'connect_args': {'connect_timeout': 10}})
        self._engine = create_engine(*args, **kwargs)

        # engine 에 의해 생성된 Session Pool 참조 바인딩
        self._pool = self._engine.pool

    @requires_init
    def get_session(self) -> SqlSession:
        """
        Data Source 로부터 가용 Connection 을 획득하고 세션 인스턴스를 반환
        :return: SqlSession
        """

        # Session(연결 제어자) 생성
        connection: Connection = self._engine.connect().execution_options(autocommit=False)
        session = SqlSession()
        session.init(self, connection)
        return session

    @requires_init
    def release_session(self, session: SqlSession):
        """
        세션을 Pool 에 반환
        :param session: AbstractSession 인스턴스
        :return: void
        """

        try:
            del session
        except (Exception, BaseException) as e:
            print(e)

    @requires_init
    def close(self):
        """
        Connection Pool 종료 처리
        !!! 어플리케이션 종료 시점에 이 메서드가 반드시 호출되어야 함 !!!
        :return: void
        """
        self._engine.dispose()  # 내부적으로 pool 객체도 함께 dispose 처리됩니다.

    def check_initialization(self):
        """
        sqlalchemy 엔진(Connection pool) 의 상태를 점검하고 이상이 있으면 예외를 발생시킵니다.
        :return: void
        """

        if not self.is_initialized:
            raise DataSourceError("database connection pool has not been initialized properly. "
                                  "Maybe something went wrong..")

    @property
    def is_initialized(self) -> bool:
        """
        sqlalchemy 엔진(Connection pool) 이 초기화되어 정상적으로 바인딩 되어있는 상태인지 여부를 bool 값으로 반환합니다.
        :return: bool
        """

        return self._engine is not None
