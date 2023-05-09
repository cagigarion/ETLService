from plugin import Plugin
import logging
import re
from rule_detection import rule_detection

class PDCombinePlugin(Plugin):
    namespace = "operator"
    name = "enrich"
    invoked: bool = False
    querys: list = []

    def load(self):
        self.invoked = True

    def show_operator(self):
        return self.name

    def build_query(self, op_specs):
        for op_spec in op_specs:
            self.querys.append(
                {"pattern": op_spec["pattern"], "field": op_spec["field"]})
        return self

    def processing_data(self, input_data):
        logging.debug(f'The data which want to parser = {input_data}')
        for query in self.querys:
            select = re.findall('{(.*?)}', query["pattern"], re.DOTALL)
            list_value = dict(zip(select, list(map(input_data.get, select))))
            value = query["pattern"].format(**list_value)
            input_data[query["field"]] = rule_detection(value)
            logging.info(
                f'Combine column in data {query["pattern"]} = {list_value}')
        return input_data  
    
