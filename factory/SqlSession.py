# -*- coding: utf-8 -*-
from typing import Callable, Iterable

from sqlalchemy.engine import Connection
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from sqlalchemy.sql import text

from common.sqlalchemist.exceptions.DataSourceError import DataSourceError


def check_needed(func: Callable) -> Callable:
    """
    이 데코레이터가 적용된 메서드는 실행에 앞서 Connection 객체의 가용성 여부를 검사합니다.
    SqlSession 이외의 다른 클래스의 메서드에 적용하면 안 됩니다.
    :param func: 적용 대상 메서드
    :return: wrapped function
    """

    def wrapper(*args, **kwargs):

        # SqlSession 이외의 다른 클래스의 메서드에 적용하면 안 됩니다.
        if not args:
            raise TypeError("this decorator should be applied to a instance method of SqlSession class.")

        # SqlSession 이외의 다른 클래스의 메서드에 적용하면 안 됩니다.
        self: SqlSession = args[0]
        if not args or not isinstance(self, SqlSession):
            raise TypeError("this decorator should be applied to a instance method of SqlSession class.")

        # 현재 커넥션의 가용성 체크
        self.check_availability()

        return func(*args, **kwargs)

    return wrapper


class SqlSession:
    """
    Sql Session 클래스.
    자신이 관리하는 _connection 에 대한 동작을 수행합니다.
    """

    # 자신을 생성한 Data Source 에 대한 참조
    _data_source = None

    # 자신이 관리하는 Connection 에 대한 참조
    _connection: Connection = None

    # 세션 객체
    _session: Session = None

    def __init__(self):
        """
        생성자 : 필요한 환경 구축 작업 수행
        """

    def __enter__(self):
        """
        with 문 (context manager) 지원을 위한 정의
        :return: SqlSession
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        with 문 (context manager) 사용을 위한 정의
        보통 with 문 context 를 벗어나면 해당 session(connection) 도 닫힐 것으로 기대하기 때문에
        기대에 부응하도록 close 처리를 명시하였습니다.
        혹여 깜빡 했을 경우를 위해서 close 전에 commit 도 한 번 호출해 주도록 하였습니다.
        :return: void
        """
        try:
            self.commit()
        except (Exception, BaseException) as e:
            print(e)
        self.close()

    def __del__(self):
        """
        소멸자 : 참조가 수거되는 시점에 호출됩니다.
        :return: void
        """
        self.close()

    def init(self, data_source, connection: Connection):
        """
        Data Source와 Connection 객체 바인딩
        :param data_source: 자신을 생성한 Data Source 객체
        :param connection: 자신이 관리하는 Connection 에 대한 참조
        :return: void
        """
        self._data_source = data_source
        self._connection = connection
        self._session = Session(self._connection)

    # @check_needed
    def commit(self):
        """
        commit
        (중요) Pool 은 항상 autocommit = False 인 connection 을 제공하기 때문에
        현재 세션을 사용했다면 명시적으로 호출해주셔야 합니다.
        만약 with 문을 활용하였다면 __exit__() 가 호출되면서 마지막에 한번 더 해주기는 합니다.
        :return: void
        """
        self._session.commit()

    # @check_needed
    def rollback(self):
        """
        rollback
        :return: void
        """
        self._session.rollback()

    def close(self):
        """
        생성된 Connection 을 Pool 에 반환합니다.
        :return: void
        """
        try:
            self._session.close()
            self._connection.close()
        except (Exception, BaseException) as e:
            raise e

    # @check_needed
    def select(self, sql: str, **params) -> dict:
        """
        Connection 객체를 통해 조회 쿼리문을 실행
        :param sql: 실행할 조회 쿼리문
        :param params: 쿼리문 구성에 필요한 파라마티
        :return: 조회 결과 dict 객체
        """

        try:
            result = self._connection.execute(text(sql), params)
            column_names = [desc[0] for desc in result.cursor.description]
            data_array = result.fetchall()
            return {'column_names': column_names, 'data': data_array}

        except SQLAlchemyError as e:
            error = e.args
            error_code = e.code
            raise DataSourceError(f"database select Error: {e}", e, error_code)

        except (Exception, BaseException) as e:
            raise e

        finally:
            pass

    # @check_needed
    def select_one(self, sql: str, **params) -> dict:
        """
        Connection 객체를 통해 조회 쿼리문을 실행하고 한 레코드만 조회
        :param sql: 실행할 조회 쿼리문
        :param params: 쿼리문 구성에 필요한 파라마티
        :return: 조회 결과 dict 객체
        """

        try:
            result = self._connection.execute(text(sql), params)
            column_names = [desc[0] for desc in result.cursor.description]
            data_array = result.fetchone()
            return {'column_names': column_names, 'data': data_array}

        except SQLAlchemyError as e:
            error = e.args
            error_code = e.code
            raise DataSourceError(f"database select Error: {e}", e, error_code)

        except (Exception, BaseException) as e:
            raise e

        finally:
            pass

    # @check_needed
    def insert(self, sql_template: str, data_list: Iterable):
        """
        Connection 객체를 통해 insert 문을 실행합니다.
        실행 성공 여부 값을 boolean 값으로 반환합니다.
        :param sql_template: 실행할 SQL 문
        :param data_list: 데이터 배열 list<list> or list<dict>
        :return: bool
        """

        result = True

        try:
            self._session.execute(text(sql_template), data_list)
            self.commit()

        except SQLAlchemyError as e:
            result = False
            error = e.args
            error_code = e.code
            raise DataSourceError(f"database select Error: {e}", error_code)

        except (Exception, BaseException) as e:
            result = False
            raise e

        finally:
            return result

    # @check_needed
    def execute(self, sql_template: str, **params):
        """
        Connection 객체를 통해 update, delete, truncate 등의 문을 실행합니다.
        실행 성공 여부 값을 boolean 값으로 반환합니다.
        :param sql_template: 실행할 SQL 문
        :param params: SQL 문 내 각 포맷 위치에 주입될 파라미터 목록
        :return: bool
        """

        result = True

        try:
            self._session.execute(text(sql_template), params)
            self.commit()

        except SQLAlchemyError as e:
            result = False
            error = e.args
            error_code = e.code
            raise DataSourceError(f"database select Error: {e}", error_code)

        except (Exception, BaseException) as e:
            result = False
            raise e

        finally:
            return result

    # @check_needed
    def execute_procedure(self, procedure_name: str, params):
        """
        todo: 작성 예정
        DB 에 저장된 프로시져를 호출하는 처리
        :param procedure_name:
        :param params:
        :return:
        """

    def get_session(self) -> Session:
        """
        현재 SqlSession 객체 자신이 관리 중인 Session 객체를 반환합니다.
        :return: Session
        """

        return self._session

    def get_connection(self) -> Connection:
        """
        현재 SqlSession 객체 자신이 관리 중인 Connection 객체를 반환합니다.
        :return: Connection
        """

        return self._connection

    def check_availability(self):
        """
        Connection 객체의 가용성 여부를 점검하고 이상이 있으면 예외를 발생시킵니다.
        :return: void
        """
        if not self.is_available:
            raise DataSourceError('Current connection is not in available state.')

    @property
    def is_available(self) -> bool:
        """
        Connection 객체의 가용성 여부 값을 boolean 값으로 반환합니다.
        :return: bool
        """
        return self._connection is not None and not self._connection.closed and not self._connection.invalidated
