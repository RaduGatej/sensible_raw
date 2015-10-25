import helpers


class Importer(object):
	def __init__(self, config):
		self.remote_db = helpers.DBHelperFactory().create_helper(config["source_db"])
		self.local_db = helpers.DBHelperFactory().create_helper(config["target_db"])
		self.indexer = helpers.FieldIndexerHelper(config["fields_to_index"])
		try:
			self.mapper = getattr(helpers, config["mapper"])()
		except:
			self.mapper = None
		self.after = None

	def process_row(self, row):
		if self.mapper:
			row = self.mapper.map(row)
		self.local_db.insert_row(self.indexer.index_fields(row))

	def import_data(self):
		self.remote_db.query_database(self.process_row, after=self.after)
		self.indexer.save_indexes()
		self.local_db.commit_changes()


class SensibleDataImporter(Importer):
	def __init__(self, config):
		Importer.__init__(self, config)
		self.last_id_db = helpers.SensibleMongoHelper(config["target_db"])
		self.current_id = self.get_last_id()
		self.after = ("id", self.current_id)

	def process_row(self, row):
		self.local_db.collection_name = row["timestamp"].strftime("%B_%Y").lower()
		if row["id"] > self.current_id:
			self.current_id = row["id"]
		Importer.process_row(self, row)

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
