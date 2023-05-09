# from pd_enrich_operator import PDEnrichPlugin
# import logging
# import json

# logging.basicConfig(format='%(asctime)s %(message)s',level=logging.INFO)
# josn_data = [{
#                 "__comment": "for a single data",
#                 "where": {
#                     "field": "revenue",
#                     "operator": ">",
#                     "value": 10000
#                 },
#                 "what":{
#                     "field": "level",
#                     "value": "High"
#                 }
#             },
#             {
#                 "__comment": "for a single data",
#                 "where": {
#                     "field": "revenue",
#                     "operator": ">",
#                     "value": 5000
#                 },
#                 "what":{
#                     "field": "level",
#                     "value": "Medium"
#                 }
#             },
#             {
#                 "__comment": "for a single data",
#                 "what":{
#                     "field": "level",
#                     "value": "Low"
#                 }
#             }]
# pd_plugin = PDEnrichPlugin()
# pd_plugin.build_query(json.loads(json.dumps(josn_data)))
# pd_plugin.processing_data({
#         "company_name": "Stevens-Barnes",
#         "place": "SaiGon",
#         "tax_number": "014715035",
#         "revenue": 13071,
#     })
# pd_plugin.processing_data({
#         "company_name": "Stevens-Barnes",
#         "place": "HaNoi",
#         "tax_number": "014715035",
#         "revenue": 5071,
#     })
# pd_plugin.processing_data({
#         "company_name": "Stevens-Barnes",
#         "place": "LongAn",
#         "tax_number": "014715035",
#         "revenue": 3071,
#     })
# pd_plugin.processing_data({
#         "company_name": "Stevens-Barnes",
#         "place": "LongAn",
#         "tax_number": "014715035",
#         "revenue": 3071,
#     })