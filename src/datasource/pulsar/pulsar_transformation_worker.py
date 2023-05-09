"""
Copyright (c) 2023, DAIENSO Lab (www.daienso.com) - All Right Reserved

Unauthorized copying, modification and distribution of source code, via any medium, is strictly prohibited. 
The code is proprietary and confidential, unless otherwise stated.

Contact contact@daienso.com for further information about the code.
"""
import pulsar
import logging
from module_loader.plugin_loader import PluginLoader
import json
import random, string

'''
GENERIC FUNCTIONS FOR DIFFERENT SCHEMAS
mockup, assume that the data is follow cvs style
'''
class PulsarTransformation:
    def __init__(self, sink_plugins, **data_config) -> None:
        self.isRunning = True
        self.sink_plugins = sink_plugins
        self.data_config = data_config
        self.op_processor = []
    
    def stopRunning(self, isRunning):
        self.isRunning = isRunning 
    
    def dt_process_json_style(self, msg, op_parsers):
        data_message = f'The data get from Pulsar = {msg.value()}'
        logging.debug(data_message)
        print(data_message)
        data=json.dumps(json.loads(msg.value()))
        data=json.loads(data)
        after_data=data.get("after")
        if after_data is not None and type(after_data) == type({}):
            data = after_data
        for func in op_parsers:
            if(data):
                try:
                    logging.debug(f'Show value of {func.show_operator()} with the data: {data}')
                    data = func.processing_data(data)
                except Exception as e:
                    logging.error(f'There is an exception when transform data = {e}')
                        
        logging.debug("Finish transformation")
        return data

    def sink_data(self, result):
        for sink_plugin in self.sink_plugins:
            try:
                sink_plugin.sink_data(**result)
            except Exception as e:
                logging.error(f'There is a error when sink data = {e}')
    
    def establish_connection(self) -> pulsar.Consumer:
        build_operation = self.data_config["build_operation"]
        connection_source = self.data_config["source"]
        ops =build_operation["ops"]
        for operator in ops:
            try:
                type_name = operator["type"]
                operator_plugin = PluginLoader().get_operator_by_name(type_name)
                if operator_plugin != None:
                    self.op_processor.append(operator_plugin.build_query(operator["value"]))
                else:
                    logging.debug(f'{operator} has not been implemented')
            except Exception as ex:
                logging.error(f'There is an error when establis connection {ex}')
        # for op,op_spec in ops.items():
        #     try:
        #         operator_plugin = PluginLoader().get_operator_by_name(op)
        #         if (operator_plugin != None):
        #             self.op_processor.append(operator_plugin.build_query(op_spec, sample_time))
        #         else:
        #             logging.debug(f'{op} has not been implemented')
        #     except Exception as ex:
        #         logging.error(f'There is an error when establis connection {ex}')
        
        '''
        ESTABLISH CONNECTOR TO PULSAR - GENERIC
        '''    
        conection = f'pulsar://{connection_source["hostname"]}:{connection_source["port"]}'
        client = pulsar.Client(conection)
        '''
        CONSUME the right data, known the schema
        '''
        # dataSchema=convert_json_to_pulsar_schema(**connection_source["schema"])
        # dataSchema = JsonSchema(CompanyRecord)
        subcription = ''.join(random.choice(string.ascii_lowercase) for i in range(10))
        consumer = client.subscribe(connection_source["topic"], subcription)
        return consumer
        
    def tranform_message_receive(self, consumer):    
        '''
        WAIT AND PROCESS DATA 
        '''
        while self.isRunning:
            '''
            The amout of data to be processed
            '''
            msgs = consumer.receive()
            try:
                '''
                MAIN TRANSFORMATION, HERE IS FUNCTION STYLE BUT IT CAN BE WORKFLOW, .... 
                '''
                ## assume that the selected data schema is avro data 
                result = self.dt_process_json_style(msgs,self.op_processor)
                ##store the result to the right data sink
                ## assume stdout 
                if(result):
                    self.sink_data(result)
            except Exception as ex:
                logging.error(f'{ex}')
                logging.debug("Continue to wait")
