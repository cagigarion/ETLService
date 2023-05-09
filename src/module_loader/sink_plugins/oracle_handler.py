from plugin import Plugin
import logging

class OraclePlugin(Plugin):
    namespace = "sink"
    name = "oracle"

    invoked: bool = False

    def load(self):
        self.invoked = True

    def config(self, **metadata):
        self.metadata = metadata
        
    def initial_connection(self):
        return "connected"
    
    def sink_data(self, **data):
        logging.info(f'Sink the data to destination Oracle: {data} ')
        return self.name