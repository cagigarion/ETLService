from plugin import Plugin
import logging
import redis
import json

class EnrichmentPlugin(Plugin):
    namespace = "operator"
    name = "enrich_temp"
    connection_cache = {}
    invoked: bool = False

    def load(self):
        self.invoked = True
    
    def show_operator(self):
        return self.name

    def build_query(self, op_specs):
        self._op_specs = op_specs
        return self
    
    def processing_data(self, input_data):
        logging.debug(f'Enrichment with the data input ={input_data}')

        '''
        HERE is a simple way to apply functions to JSON data
        using query mechanism. Of course the key point is how to do it fast and how to support many types of json data and functions
        '''
        for _op_spec in self._op_specs:
            reference_data = None
            reference_type = _op_spec["what"]["reference_type"]
            
            if(reference_type.lower() == "redis"):
                reference_data = self.redis_query_data(_op_spec)
            elif(reference_type.lower() == "internal"):
                reference_data = self.internal_query_data(_op_spec)
            else:
                logging.warning(f'Have not supported reference type yet: {reference_type}')

            logging.debug(f'Enrichment with the reference_data={reference_data}')
            if (reference_data):
                #reference data must be stored into new field
                input_data[_op_spec["what"]["added_field_name"]]=reference_data
            logging.debug(f'Enrichment with the data output ={input_data}')
        return input_data
    
    def redis_query_data(self, _op_spec):
        try:
            what_enrichment_op=_op_spec["what"]
            #check if the the data met condition
            logging.debug(f'By pass the check if the data meets the where {_op_spec["where"]}')
            #if yes, get reference data
            reference_id=what_enrichment_op["reference_id"]
            #just try to use cache, simple idea to avoid reconnection
            if (reference_id not in self.connection_cache.keys()):
                #for testing the reference id is a file to have secret
                #so load it. usually this should be loaded from vault
                with open(reference_id) as redis_conf_file:
                    redis_conf= json.load(redis_conf_file)
                #of course we dont make new connection if the connection is already established
                redis_connection=redis.Redis(host=redis_conf["host"],port=redis_conf["port"],password=redis_conf["password"],decode_responses=True)
                self.connection_cache[reference_id]=redis_connection
            else:
                redis_connection=self.connection_cache[reference_id]
            #redis enrichment simply calls get with the query (key) to get a reference data
            query=what_enrichment_op["query"]
            logging.debug(f'Get reference data with query={query}')
            """
            TODO: conceptually, the query can be changed, e.g., when it is defined based on values of streaming data.
            However, if the query is static and the reference data wont change, it is also important to cache the reference data
            so that we dont have to call it again and again
            """
            reference_data= redis_connection.get(query)
            return reference_data
        except Exception as e:
            logging.error(f'There is an error when get data reference from Redis cache: {e}')
            return None
        
    def internal_query_data(self, _op_spec):
        try:
            return _op_spec["what"]["reference_data"]
        except Exception as e:
            logging.error(f'There is an error when get data reference from internal: {e}')
            return None
        

