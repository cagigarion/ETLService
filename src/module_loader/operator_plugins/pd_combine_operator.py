from plugin import Plugin
import logging
import re

class PDCombinePlugin(Plugin):
    namespace = "operator"
    name = "pd_combine"
    invoked: bool = False

    def load(self):
        self.invoked = True

    def show_operator(self):
        return self.name
    
    def build_query(self, **op_spec):
        self.pattern = op_spec["pattern"]
        self.field = op_spec["field"]
        return self
    
    def processing_data(self, input_data):
        logging.debug(f'The data which want to parser = {input_data}')
        select = re.findall('{(.*?)}', self.pattern, re.DOTALL)
        list_value = dict(zip(select, list(map(input_data.get, select))))
        input_data[self.field] = self.pattern.format(**list_value)
        logging.info(f'Combine column in data {self.pattern} = {list_value}')
        return input_data