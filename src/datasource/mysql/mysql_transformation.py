import logging
import json
import mysql.connector
import time
from module_loader.plugin_loader import PluginLoader

class MySQLTransformation:
    def  __init__(self, sink_plugins, **data_config) -> None:
        self.isRunning = True
        self.sink_plugins = sink_plugins
        self.data_config = data_config
        self.op_processor = []
        self.limit= 20
        self.offset=0

    def process_ops(self, data, op_parsers):
        for func in op_parsers:
            if(data):
                try:
                    logging.debug(f'Show value of {func.show_operator()} with the data: {data}')
                    data = func.processing_data(data)
                    return data
                except Exception as e:
                    logging.error(f'There is an exception when transform data = {e}')
        
        

    def sink_data(self, result):
        for sink_plugin in self.sink_plugins:
            try:
                sink_plugin.sink_data(**result)
            except Exception as e:
                logging.error(f'There is a error when sink data = {e}')
        pass

    def establish_connection(self):
        '''
        build operator 
       
        '''
        build_operator = self.data_config["build_operation"]
        ops = build_operator["ops"]
        for op,op_spec in ops.items():
            try:
                operator_plugin = PluginLoader().get_operator_by_name(op)
                if (operator_plugin != None):
                    self.op_processor.append(operator_plugin.build_query(op_spec))
                else:
                    logging.debug(f'{op} has not been implemented')
            except Exception as ex:
                logging.error(f'There is an error when establis connection {ex}')
        
    def tranform_message_receive(self):
        connection_source = self.data_config["source"]  
        #check connection
        config = {
            'user': connection_source["user"],
            'password': connection_source["password"],
            'host': connection_source["host"],
            'port': connection_source["port"],
            'database': connection_source["database"],
            'raise_on_warnings': True
        }
        self.cnx = mysql.connector.connect(**config)
        
        while True:
            # transform data
            mysql_cursor = self.cnx.cursor(dictionary=True)
            select_table = connection_source["schema"]["table"]
            select_columns = connection_source["schema"]["select"]
            mysql_cursor.execute(f"SELECT {','.join(select_columns)} FROM {select_table} LIMIT {self.limit} offset {self.offset};")
            
            myresult = mysql_cursor.fetchall()
            for x in myresult:
                #check operator
                result = self.process_ops(x, self.op_processor)
                if result:
                    self.sink_data(x)
            self.offset = self.limit + 1

            time.sleep(10)
       
    