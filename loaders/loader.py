from monary import Monary

config = {
	"calllog": {
		"field_names": ["user", "timestamp", "called_user", "number", "duration", "type"],
		"field_types": ["int16", "int32", "int16", "string:40", "int16", "int8"]
	},

	"sms": {
		"field_names": ["user", "timestamp", "called_user", "number", "body", "type"],
		"field_types": ["int16", "int32", "int16", "string:40", "string:40", "int8"]
	},

	"bluetooth": {
		"field_names": ["user", "timestamp", "seen_user", "level"],
		"field_types": ["int16", "int32", "int16", "int16"]
	},

	"location": {
		"field_names": ["user", "timestamp", "lat", "lon"],
		"field_types": ["int16", "int32", "float64", "float64"]
	},

	"wifi": {
		"field_name": ["user", "timestamp", "bssid", "ssid", "level"],
		"field_types": ["int16", "int32", "int32", "int32", "int16"]
	}

}


def load_data(data_type, month):
	return load_from_db(data_type, month, config[data_type]["field_names"], config[data_type]["field_types"])


def load_from_db(db, collection, field_names, field_types):
	with Monary("127.0.0.1") as monary:
		arrays = monary.query(
			db,  # database name
			collection,  # collection name
			{},  # query spec
			field_names,  # field names (in Mongo record)
			field_types  # Monary field types (see below)
		)

	return arrays