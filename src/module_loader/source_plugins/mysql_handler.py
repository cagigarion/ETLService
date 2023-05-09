import logging
import mysql.connector
import time
from plugin import Plugin
from mysql.connector import errorcode
from datasource.mysql.mysql_transformation import MySQLTransformation

class SourceMysqlPlugin(Plugin):
    namespace = "source"
    name = "mysql"
    invoked: bool = False
    limit: int = 0
    offset: int = 0

    def load(self):
        self.invoked = True
    
    def load_data(self):
        return self.name

    def set_sink_plugins(self, sink_plugins):
        self.sink_plugins = sink_plugins
    
    def handler(self, **data_config):
        try:
            # print("data_config", data_config)
            # connection_source = data_config["source"]
            # print(connection_source["host"])
            #check connection
            # config = {
            #     'user': connection_source["user"],
            #     'password': connection_source["password"],
            #     'host': connection_source["host"],
            #     'port': connection_source["port"],
            #     'database': connection_source["database"],
            #     'raise_on_warnings': True
            # }

            # self.cnx = mysql.connector.connect(**config)
            # print("config", config)
            

            
            mysql_transform = MySQLTransformation(self.sink_plugins, **data_config)
            mysql_transform.establish_connection()
            mysql_transform.tranform_message_receive()

           

        except Exception as e:
            
            logging.error(f'Have not run the tranform data yet: {e}')

    