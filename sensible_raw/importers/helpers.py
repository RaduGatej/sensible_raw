from collections import defaultdict
import json
import logging
import os

import MySQLdb
import MySQLdb.cursors
from pymongo import MongoClient
from sqlalchemy import *
from sqlalchemy.sql import select
import time
import decimal

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')


class MySQLHelper(object):
	def __init__(self, config):
		self.hostname = config["hostname"]
		self.user = config["user"]
		self.password = config["password"]
		self.database = config["database"]
		self.table_name = config["table"]
		self.query_fields = config["query_fields"]

	def __get_connection(self):
		connection_string = 'mysql://%s:%s@%s/%s' % (self.user, self.password, self.hostname, self.database)
		engine = create_engine(connection_string, connect_args={'cursorclass': MySQLdb.cursors.SSCursor})
		connection = engine.connect()
		return connection

	def query_database(self, process_row_function, after=None):
		connection = self.__get_connection()
		metadata = MetaData(bind=connection.engine)
		table = Table(self.table_name, metadata, autoload=True)
		query = select([getattr(table.c, field) for field in self.query_fields])
		if after:
			field, value = after
			query = query.where(getattr(table.c, field) > value)
		result = connection.execute(query)
		for row in result:
			row_dict = {}
			for index, item in enumerate(list(row)):
				if isinstance(item, decimal.Decimal):
					item = float(item)
				row_dict[self.query_fields[index]] = item
			process_row_function(row_dict)


class SensibleMongoHelper(object):
	INSERT_BATCH_SIZE = 100000

	def __init__(self, config):
		self.client = MongoClient()
		self.client.admin.authenticate(config["user"], config["password"])
		self.db = self.client[config["database"]]
		self.insert_batch = defaultdict(list)
		self.collection_name = config["table"]

	def insert_row(self, row):
		self.insert_batch[self.collection_name].append(row)
		if len(self.insert_batch[self.collection_name]) == self.INSERT_BATCH_SIZE:
			self.db[self.collection_name].insert(self.insert_batch[self.collection_name])
			logging.warning("Inserted %s in mongo", len(self.insert_batch[self.collection_name]))
			self.insert_batch[self.collection_name] = []

	def commit_changes(self):
		for collection, batch in self.insert_batch.items():
			self.db[collection].insert(batch)
			logging.warning("Inserted %s in mongo", len(batch))
		self.insert_batch = defaultdict(list)

	def query_database(self):
		return [doc for doc in self.db[self.collection_name].find()]


class FieldIndexerHelper():
	DEFAULT_INDEX_FOLDER = "indices"

	def __init__(self, fields_to_index, index_folder=DEFAULT_INDEX_FOLDER, start_value=0):
		self.index_folder = index_folder
		self.field_indices = defaultdict(lambda: defaultdict(self.__integer_field_index))
		self.__load_indices()
		self.index_counters = defaultdict(lambda: start_value)
		self.__load_index_counters()
		self.fields_to_index = fields_to_index
		self.current_field = None

	def __load_index_counters(self):
		for index_name, indices in self.field_indices.items():
			self.index_counters[index_name] = max(indices.values())

	def __load_indices(self):
		for filename in os.listdir(self.index_folder):
			index = json.loads(open(self.index_folder + "/" + filename, "r").read())
			index_name = filename.split(".")[0]
			self.field_indices[index_name] = defaultdict(self.__integer_field_index, index)

	def index_fields(self, row):

		for field, index_name in self.fields_to_index:
			value_to_index = row[field]
			if not isinstance(value_to_index, basestring):
				continue
			self.current_field = field
			row[field] = self.field_indices[index_name][value_to_index]

		return row

	def save_indexes(self):
		for index_name, indices in self.field_indices.items():
			print indices
			f = open(self.index_folder + "/" + index_name + ".json", "w")
			f.write(json.dumps(dict(indices)))
			f.close()

	def __integer_field_index(self):
		self.index_counters[self.current_field] += 1
		return self.index_counters[self.current_field]


class BluetoothMacMapper(object):
	def __init__(self):
		self.device_inventory = json.loads(open("device_inventory", "r").read())
		self.mac_indexer = FieldIndexerHelper([["bt_mac", "bt_mac"]], index_folder="mac_mapper", start_value=10000)

	def map_bt_mac_to_user(self, bt_mac, timestamp):
		inventory_entries = self.device_inventory.get(bt_mac)
		if not inventory_entries:
			return False, bt_mac

		for entry in inventory_entries:
			if entry['start'] <= int(time.mktime(timestamp.timetuple())) <= entry['end']:
				return True, entry['user']

		return False, bt_mac

	def map(self, row):
		is_username, mapped_value = self.map_bt_mac_to_user(row["bt_mac"], row["timestamp"])
		if not is_username:
			return self.mac_indexer.index_fields(row)
		return row

	def commit(self):
		self.mac_indexer.save_indexes()

	def map(self, row):
		row["bt_mac"] = self.map_bt_mac_to_user(row["bt_mac"], row["timestamp"])
		return row


class PhoneNumberMapper(object):
	def __init__(self):
		self.phone_book = json.loads(open("phone_book", "r").read())

	def map(self, row):
		if not self.phone_book.get(row['number']):
			return row
		row["number"] = self.phone_book.get(row["number"])
		return row


class CSVHelper(object):
	INSERT_BATCH_SIZE = 100

	def __init__(self, config):
		self.db = config["database"]
		self.insert_batch = defaultdict(list)
		self.collection_name = config["table"]
		self.open_files = {}

	def insert_row(self, row):
		self.insert_batch[self.collection_name].append(",".join([str(value) for value in row.values()]))
		filename = self.db + "_" + self.collection_name
		if filename not in self.open_files:
			self.open_files[filename] = open(filename, "a")
		if len(self.insert_batch[self.collection_name]) == self.INSERT_BATCH_SIZE:
			f = self.open_files[filename]
			f.write("\n".join(self.insert_batch[self.collection_name]) + "\n")
			f.flush()
			logging.warning("Inserted %s in file", len(self.insert_batch[self.collection_name]))
			self.insert_batch[self.collection_name] = []

	def commit_changes(self):
		for collection, batch in self.insert_batch.items():
			f = self.open_files[self.db + "_" + collection]
			f.write("\n".join(batch) + "\n")
			f.flush()
			logging.warning("Inserted %s in file", len(batch))
		self.insert_batch = defaultdict(list)
		for f in self.open_files.values():
			f.close()


class DBHelperFactory(object):
	def __init__(self):
		self.db_helpers = {"csv": CSVHelper, "mysql": MySQLHelper, "mongo": SensibleMongoHelper, "json": JSONHelper}

	def create_helper(self, db_config):
		return self.db_helpers[db_config["db_type"]](db_config)


class JSONHelper(object):
	def __init__(self, config):
		self.source_file = config["source_file"]

	def query_database(self, process_row_function):
		data = json.loads(open(self.source_file, "r").read())
		for doc in data:
			process_row_function(doc)


