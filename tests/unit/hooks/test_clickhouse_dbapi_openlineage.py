import unittest
from unittest import mock

from airflow.models import Connection

from airflow_clickhouse_plugin.hooks.clickhouse_dbapi import ClickHouseDbApiHook


class ClickHouseDbApiHookOpenLineageTestCase(unittest.TestCase):
    def test_get_openlineage_database_info(self):
        hook = ClickHouseDbApiHook()

        conn = Connection(host='11.22.33.44', login='user', password='pass', schema='mydb')
        database_info = hook.get_openlineage_database_info(conn)
        self.assertEqual(database_info.scheme, 'clickhouse')
        self.assertEqual(database_info.authority, '11.22.33.44:9000')

        conn_with_port = Connection(host='11.22.33.44', port=9001, login='user', password='pass', schema='mydb')
        database_info = hook.get_openlineage_database_info(conn_with_port)
        self.assertEqual(database_info.scheme, 'clickhouse')
        self.assertEqual(database_info.authority, '11.22.33.44:9001')

    def test_get_openlineage_default_schema(self):
        self.assertEqual(ClickHouseDbApiHook().get_openlineage_default_schema(), 'default')
        self.assertEqual(ClickHouseDbApiHook(schema='mydb').get_openlineage_default_schema(), 'mydb')



if __name__ == '__main__':
    unittest.main()
