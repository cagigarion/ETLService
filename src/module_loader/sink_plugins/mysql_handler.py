from plugin import Plugin
import logging
import mysql.connector
from mysql.connector import errorcode

# this plugin is discoverable since it can be resolved into a PluginSpec
class MysqlPlugin(Plugin):
    namespace = "sink"
    name = "mysql"

    invoked: bool = False

    def load(self):
        self.invoked = True

    def config(self, **metadata):
        self.metadata = metadata
        
    def initial_connection(self):
        ssl_disabled = True
        if self.metadata.get("ssl") is not None:
            ssl_disabled = self.metadata["ssl"] == False
        config = {
            'user': self.metadata["user"],
            'password': self.metadata["password"],
            'host': self.metadata["host"],
            'port': self.metadata["port"],
            'database': self.metadata["database"],
            'raise_on_warnings': True,
            'ssl_disabled':ssl_disabled  
        }
        
        # debug
        print(config)

        self.cnx = mysql.connector.connect(**config)
        self.create_tables()
        
    def create_tables(self):
        cursor = self.cnx.cursor()
        tables_sql = self.metadata["schema"]["create_tables"]
        for table_script in tables_sql:
            try:
                cursor.execute(table_script)
            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                    logging.debug(f"Table already exists.")
                else:
                    logging.error(f'Have an error occur: {err.msg}')
            else:
                logging.debug(f"Run table script successfully: {tables_sql}")
    
    def sink_data(self, **data):
        logging.info(f'Sink the data to destination MySQL: {data} ')
        
        cursor = self.cnx.cursor()
        try:
            sink_to_table = self.metadata["schema"]["sink_to"]
            select_columns = self.metadata["schema"]["select"]
            mydict = {}
            for column_name in select_columns:
                mydict[column_name] = data.get(column_name)
            logging.info(f'Insert data to table = {list(mydict.values())}')
            placeholders = ', '.join(['%s'] * len(mydict))
            columns = ', '.join(mydict.keys())
            sql = "INSERT INTO %s ( %s ) VALUES ( %s )" % (sink_to_table, columns, placeholders)
            cursor.execute(sql, list(mydict.values()))
            self.cnx.commit()
        except Exception as e:
            logging.error(f'Cannot sink data {data} to database mysql = {e}')
        