from plugin import Plugin
import logging
import mysql.connector
from mysql.connector import errorcode

# this plugin is discoverable since it can be resolved into a PluginSpec
class LogFilePlugin(Plugin):
    namespace = "sink"
    name = "logfile"

    invoked: bool = False

    def load(self):
        self.invoked = True

    def config(self, **metadata):
        self.metadata = metadata
        
    def initial_connection(self):
        self.file_name = self.metadata["schema"]["filename"]
        self.log_path = self.metadata["schema"]["logpath"]
    
    def sink_data(self, **data):
        logging.info(f'Sink the data to destination Logfile: {data} ')
        try:
            #write file to /dataconfig
            with open(f'{self.log_path}/{self.file_name}', 'a+') as f:
                f.write(f'{data}\r\n')
        except Exception as e:
            logging.error(f'Cannot sink data {data} to log file: {e}')
        