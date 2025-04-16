"""
This file contains:

data_extracts: A list of dictionaries in the format specified by MasterTax.
column_names: A dictionary of key-value pairs.
    Keys are layout names from data extracts (e.g. tagValues from {"tagCode": "LAYOUT_NAME", "tagValues": ["EXTRACT_01"]})
    Values are lists of column names in the same order as defined in the data extracts.
"""

data_extracts = [
                {"processNameCode": {"code": "DATA_EXTRACT"},
                  "processDefinitionTags": [{"tagCode": "LAYOUT_NAME", "tagValues": ["EXTRACT_01"]},
                                            {"tagCode": "FILTER_NAME", "tagValues": ["EXTRACT_01_FILTER"]}],
                  "filterConditions": [{"joinType": "oneOf", "attributes": [
                      {"attributeID": "COLUMN_03", "operator": "gt", "attributeValue": ["0"]}]}]}]

column_names = {
      "EXTRACT_01": ["ID", "COLUMN_01", "COLUMN_02", "COLUMN_03"]}