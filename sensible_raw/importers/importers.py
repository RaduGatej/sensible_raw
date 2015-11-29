import helpers
import logging


logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')

class Importer(object):
	def __init__(self, config):
		self.remote_db = helpers.DBHelperFactory().create_helper(config["source_db"])
		self.local_db = helpers.DBHelperFactory().create_helper(config["target_db"])
		self.indexer = helpers.FieldIndexerHelper(config["fields_to_index"])
		try:
			self.mapper = getattr(helpers, config["mapper"])()
		except BaseException, e:
			logging.warn("Couldn't load mapper due to following error: " + e.message)
			self.mapper = None

		try:
			self.row_expander = getattr(helpers, config["expander"])()
		except BaseException, e:
			self.row_expander = None
			logging.warn("Couldn't load expander due to following error: " + e.message)
		self.after = None

	def process_row(self, row):
		if self.mapper:
			row = self.mapper.map(row)
		if self.expand_row(row):
			return

		self.local_db.insert_row(self.indexer.index_fields(row))

	def expand_row(self, main_row):
		if self.row_expander:
			expanded_row = []

			try:
				expanded_row = self.row_expander.expand(main_row)
			except helpers.RowExpanderException, e:
				#logging.error("Exception while expanding row: " + str(e.message))
				return False

			for row in expanded_row:
				self.process_row(row)

			return True
		return False

	def import_data(self):
		self.remote_db.query_database(self.process_row, after=self.after)
		self.indexer.save_indexes()
		if self.mapper:
			self.mapper.commit()
		self.local_db.commit_changes()


class SensibleDataImporter(Importer):
	def __init__(self, config):
		Importer.__init__(self, config)
		self.last_id_db = helpers.SensibleMongoHelper(config["target_db"])
		self.current_id = self.get_last_id()
		self.after = ("id", self.current_id)

	def process_row(self, row):
		self.local_db.collection_name = row["timestamp"].strftime("%B_%Y").lower()
		Importer.process_row(self, row)
		if row["id"] > self.current_id:
			self.current_id = row["id"]

	def import_data(self):
		Importer.import_data(self)
		self.last_id_db.insert_row({"scan_id": self.current_id})
		self.last_id_db.commit_changes()

	def get_last_id(self):
		self.last_id_db.collection_name = "last_scan_ids"
		last_ids = self.last_id_db.query_database()
		if not last_ids:
			return 0
		return max(last_ids, key=lambda x: x["scan_id"])["scan_id"]
