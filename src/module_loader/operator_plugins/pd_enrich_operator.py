from plugin import Plugin
import logging
import json
from jsonpath_ng.ext import parse
import pandas as pd
from pandas import Timestamp
from numpy import nan
import numpy as np

class PDEnrichPlugin(Plugin):
    namespace = "operator"
    name = "pd_match1"
    query = []
    invoked: bool = False

    def load(self):
        self.invoked = True

    def show_operator(self):
        return self.name
    
    def build_query(self, op_spec):
        logging.info(op_spec)
        #each function is carried out by a processor. here is one example based on JSONPath
        for op in op_spec:
            if("where" in op):
                op_where = op["where"]
            # if (op_where): 
                logic_where = f'dfLookup.query("{op_where["field"]} {op_where["operator"]} {op_where["value"]}")'
            op_what = op["what"]
            if (op_what):
                logic_what = f'dfLookup["{op_what["field"]}"] = "{op_what["value"]}"'
            self.query.append({"where": logic_where, "what": logic_what})
        return self
    
    def processing_data(self, input_data):
        logging.debug(f'The data which want to parser = {input_data}')
        # Data cleaning
        dfLookup = pd.DataFrame(input_data, [0])
        # for query in self.query:
        #     if (query["where"]):
        #         logging.info(f'where operators = {query["where"]}')
        #         eval(query["where"])
        #     if (query["what"]):
        #         logging.info(f'where operators = {query["what"]}')
        #         eval(query["what"])
        # selecting rows based on condition 
        # rslt_df = eval ("dfLookup[dfLookup['revenue'].isin(options)]")
        # logging.info(eval('dfLookup.query("revenue > 10000")'))
        # logging.info(eval('dfLookup.query(dfLookup["revenue"] > 10000)'))
        newString = 'aaaaaaaaaaaaaaaaaaaaa{}{}{}'
        dfLookup['combined'] = dfLookup.apply(lambda x: newString.format(*[x['company_name'], x['place'], x['revenue']]), axis=1)
        dfLookup['level'] = np.where(dfLookup['revenue'] > 10000, 'High', np.where(dfLookup['revenue'] > 5000, 'Medium', 'Low'))
        # More data cleaning
        logging.info(f'Enrich column level in data = {dfLookup.to_json()}')