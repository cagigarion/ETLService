from plugin import Plugin
import logging
import json
import requests
from rule_detection import datetime_rule_detection


class ApiPDCombinePlugin(Plugin):
    namespace = "operator"
    name = "enrich_api"
    invoked: bool = False
    querys: list = []

    def load(self):
        self.invoked = True

    def show_operator(self):
        return self.name

    def build_query(self, op_specs):
        for op_spec in op_specs:
            op_spec_fields = op_spec["response_schema"]["fields"]
            fields = []
            for op_spec_field in op_spec_fields:
                fields.append(
                    {"pattern": op_spec_field["pattern"], "field": op_spec_field["name"]})
            self.querys.append(
                {
                    "api_link": op_spec["api_link"],
                    "method": op_spec["method"],
                    "body": op_spec["body"],
                    "fields": fields
                }
            )
        return self

    def operator_url(self, url):
        end_operator = ")"
        start_operators = [
            "add_seconds(", "add_minutes(", "add_hours(", "sub_seconds("]
        for start_operator in start_operators:
            index_start_operator = url.find(start_operator)
            while index_start_operator > 0:
                index_end_operator = url.find(end_operator)
                operator = url[index_start_operator: index_end_operator+1]
                logging.info("operator value:"+operator)
                url = url[0:index_start_operator] + \
                    datetime_rule_detection(operator).strftime("%Y-%m-%dT%H:%M:%S.%fZ")+ \
                    url[index_end_operator+1:]
                index_start_operator = url.find(start_operator)

        return url

    def calling_api(self, query, input_data):
        try:
            url = self.operator_url(query["api_link"].format(**input_data))
            logging.info("link api:" + url)
            method = query["method"]
            if method.upper() == "GET":
                response = requests.get(
                    url, headers={"Content-Type": "application/json"})
                logging.info("The API Called Get")
                
            print(response)
            content = response.content
            if content is None:
                return
            result = json.loads(content.decode())
            return result
        except Exception as ex:
            logging.info("Call API is error")
            logging.error(f'{ex}')

    def processing_data(self, input_data):
        logging.debug(
            f'EnrichAPI: The data which want to parser = {input_data}')
        for query in self.querys:
            response_data = self.calling_api(query, input_data)
            if response_data is None:
                continue

            fields = query["fields"]
            for field in fields:
                pattern = field["pattern"]

                arrPattern = pattern.split('.')
                len_arr = len(arrPattern)
                value_pattern = response_data
                for index in range(len_arr):
                    field_name = arrPattern[index]
                    if field_name[0] == "[":
                        field_name = int(field_name[1: len(field_name)-1])

                    if index == len_arr - 1:
                        input_data[field["field"]
                                   ] = value_pattern.get(field_name)
                    elif index == 0:
                        value_pattern = response_data[field_name]
                    else:
                        value_pattern = value_pattern[field_name]
                        if value_pattern is None:
                            break
        return input_data
