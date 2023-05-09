from plugin import Plugin
from datasource.pulsar.pulsar_transformation_worker import PulsarTransformation
import logging

class PulsarPlugin(Plugin):
    namespace = "source"
    name = "pulsar"

    invoked: bool = False

    def load(self):
        self.invoked = True

    def load_data(self):
        return self.name
    
    def set_sink_plugins(self, sink_plugins):
        self.sink_plugins = sink_plugins
    
    def handler(self, **data_config):
        try:
            pulsar_tranform = PulsarTransformation(self.sink_plugins, **data_config)
            consumer = pulsar_tranform.establish_connection()
            if (consumer != None):
                pulsar_tranform.tranform_message_receive(consumer)
        except Exception as e:
            logging.error(f'Have not run the tranform data yet: {e}')
        