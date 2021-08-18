from elasticsearch import Elasticsearch
import json
import pandas as pd

# =============================================================================
#  Schema / Index
# =============================================================================
def create_index(es, INDEX, properties, types):
	mapping_obj = __get_mappings(properties, types)
	if mapping_obj is not None:
		init_index_obj = {"settings": __get_settings(),
			              "mappings": mapping_obj}
		print("[TEST]\n", json.dumps(init_index_obj))
		es.indices.create(index=INDEX, body=init_index_obj)
	else:
		print("[WARNING] Fail to execute func: `create_index` "+\
		      "because `mapping_obj` is null.")

def __get_settings():
	settings = {
		"index": {
			"number_of_shards": 3,
			"number_of_replicas": 2
		}
	}
	return settings

def __get_mappings(properties, types):
	mappings = {"properties": dict()}
	for property_, type_ in zip(properties, types):
		mappings["properties"][property_] = {"type": type_}
	return mappings

# =============================================================================
#  Record / Type
# =============================================================================
def fill_data(es, INDEX, data_df, properties):
	mappings = __csv_to_mappings(data_df, properties)
	for es_id, mapping in enumerate(mappings):
		es.index(index=INDEX, body=mapping, id=es_id)

def __csv_to_mappings(data_df, properties):
	mappings = [dict(zip(properties,
					     [e.strip() 
		                  if type(e).__name__=="str"
		                  else e
		                  for e in list(data_df.iloc[i, :])]
						 ))
			    for i in range(len(data_df))]
	return mappings

def get_all_docs(es, INDEX):
	matched_records = None 
	query = {
		        "query": {
                    "match_all": {}
		        }
			}
	result = es.search(index=INDEX, body=query)
	matched_records = ['\n'.join([f"{k}: {v}" 
					               for k, v in record["_source"].items()])
					   for record in result["hits"]["hits"]]
	return matched_records
	
def search(es, INDEX, properties, by, val):
	matched_records = None 
	if by in properties:
		print(f"[INFO] Search for document(s) with `{by}`: \"{val}\".")
		if type(val).__name__ == "int": 
			query = {
		        "query": {
		            "bool": {
		                "must": {
		                    "term": {
		                        by: val
		                    }
		                }
		            }
		        }
			}

		elif type(val).__name__ == "str":
			query = {
		        "query": {
                    "match": { 
						by: val
			        }
		        }
			}
		result = es.search(index=INDEX, body=query)
		matched_records = ['\n'.join([f"{k}: {v}" 
					                  for k, v in record["_source"].items()])
					       for record in result["hits"]["hits"]]
	else:
		print("[WARNING] Fail to execute func: `searchs` "+\
		      "because parameter: `by` is invalid.")
	return matched_records

def multi_search(es, INDEX, by_properties, vals):
	matched_records = None 
	print(f"[INFO] Search for document(s) with `{by_properties}`: \"{vals}\".")
	'''
	query = {
        "query": {
            "bool": {
                "should": [
                    {"term": {
                        "age": 22
                    }},
					{"term": {
                        "age": 20
                    }},
					{"match": {
						"name": "正"	
					}}
                ]
            }
        }
	}
	'''
	conditions = list()
	for by_property, val_ in zip(by_properties, vals):
		if type(val_).__name__ == "int":
			conditions.append({"term": {by_property: val_}})
		elif type(val_).__name__ == "str":
			conditions.append({"match": {by_property: val_}})
		elif type(val_).__name__ == "list": 
			for e in val_:
				if type(e).__name__ == "int":
					conditions.append({"term": {by_property: e}})
				elif type(e).__name__ == "str":
					conditions.append({"match": {by_property: e}})
	query = {"query": {"bool": {"should": conditions}}}
	print(f"[INFO] query:\n{query}")
	result = es.search(index=INDEX, body=query)
	matched_records = ['\n'.join([f"{k}: {v}" 
				                  for k, v in record["_source"].items()])
				       for record in result["hits"]["hits"]]
	return matched_records

if __name__ == "__main__":
	'''
	ES_HOST = "192.168.1.59"
	ES_PORT = "9200"
	'''
	ES_HOST = "127.0.0.1"
	ES_PORT = "9200"
	#-------------------------
	#INDEX = "school_members"
	#INDEX = "test_school_members_awa"
	INDEX = "awa__"
	es = Elasticsearch(hosts=ES_HOST, port=ES_PORT)
	#-------------------------
	data_csv_path = "res/test_students.csv"
	schema_csv_path = "res/index_schema.csv"
	#-------------------------
	'''
	"text" : 部分相同便可被 match 搜尋到
	"keyword" : 需要全部相同(一字不差)才可被 match 搜尋到
	'''
	#properties = ["sid", "name", "age", "class"]
	#types = ["integer", "text", "integer", "keyword"]
	data_df = pd.read_csv(data_csv_path, encoding="utf-8-sig")
	properties = [e.strip() for e in list(data_df.columns)] 
	
	schema_df = pd.read_csv(schema_csv_path, encoding="utf-8-sig").iloc[:len(properties),:]
	types = [e.strip() for e in list(schema_df.iloc[:,1])]
	
	''' 1. List index & all documents  '''
	if es.indices.exists(index=INDEX):
		print(f"[INFO] List existing index `{INDEX}` ...\n")
		print(es.indices.get(INDEX)[INDEX]["mappings"]["properties"], '\n')
		
		all_docs = get_all_docs(es, INDEX)
		print(f"[INFO] There're {len(all_docs)} documents found.\n")
		print("all documents:", *(e for e in all_docs), sep='\n'*2, end='\n'*2)
	else:
		print("[INFO] The index is not existing.\n")
	
	''' 2. Create index (if index hasn't exists) / Search (otherwise) '''
	if not es.indices.exists(index=INDEX):
		''' 2.a Create a new index '''
		print("[INFO] Creating index ...\n")
		create_index(es, INDEX, properties, types)
		
		''' 2.b Fill data '''
		print("[INFO] Filling data ...\n")
		fill_data(es, INDEX, data_df, properties)
		print("[INFO] Successfully prepared all data.\n")
	else:
		print("[INFO] Elasticsearch object already exists.\n")
		print("[INFO] Searching ...\n")
		
		''' [TEST] Query index & Show result '''
		matched_records = multi_search(es, INDEX, ["name", "age"], [["連", "正"], [22, 21]])
		#matched_records = multi_search(es, INDEX, ["name", "age"], ["正", 22])
		#matched_records = search(es, INDEX, properties, by="age", val=21)
		#matched_records = search(es, INDEX, properties, by="class", val="四資管三")
		#matched_records = search(es, INDEX, properties, by="name", val="正")
		if matched_records is not None:
			print("num. of matched records: ", len(matched_records), sep='')
		print("matched records:", *(e for e in matched_records), sep='\n'*2, end='\n'*2)

	''' 3. Delete indeices '''
	existing_indices = list(es.indices.get('*').keys())
	#print(es.indices.get_alias("*"))
	print(f"[INFO] existing_indices:\n{existing_indices}\n")
	
	indices_to_del = []
	for index_to_del in indices_to_del:
		if es.indices.exists(index=index_to_del):
			print(f"[INFO] List documents of index `{index_to_del}` ...\n")
			print(es.indices.get(index_to_del)[index_to_del]["mappings"]["properties"], '\n')
			ans = input(f"Do you want to delete index `{index_to_del}`?\n"+\
				         "Press q to quit.\n(y/n): ").strip().lower()
			if ans in ('y', 'n', 'q'):
				if ans == 'q':
					print("[INFO] Stopping deletion process ...")
					break
				elif ans == 'y':
					es.indices.delete(index=index_to_del)
				else:
					continue
			else:
				print("[WARNING] Input is invalid.\n"+\
			          "Stopping deletion process ...\n")
				break
		else:
			print(f"[WARNING] Index `{index_to_del}` does not exist.\n")
