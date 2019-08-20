import pyodbc


class Query:
    def __init__(self, database):
        # Microsoft SQL Server credentials
        self.user = ''
        self.password = ''
        self.database = database
        self.server = r''
        self.driver = '{SQL Server}'
        # Connection
        self.cnxn = pyodbc.connect(
            f'DRIVER={self.driver};SERVER={self.server};DATABASE={self.database};UID={self.user};PWD={self.password}')
        self.cursor = self.cnxn.cursor()

    def select(self, table, headers='*', where=''):
        sep_headers = ','
        str_headers = headers if type(headers) == str else sep_headers.join(headers)
        if where != '':
            where = 'WHERE ' + where
        str_select = 'SELECT {0} FROM {1} {2}'.format(str_headers, table, where)
        # print(str_select)
        self.cursor.execute(str_select)
        # dato = []
        dato = self.cursor.fetchall()
        return dato

    def insert_other(self, str_insert):
        # print(str_insert)
        self.cursor.execute(str_insert)
        self.cnxn.commit()

    def close(self):
        self.cursor.close()
