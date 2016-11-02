# Data Pipeline Avro Util


What is it?
-----------
The Data Pipeline Avro utility package provides a Pythonic interface
for reading and writing Avro schemas. It also provides an enum class
for metadata that we've found useful to include in our schemas.


How to download
---------------
```
git clone git@github.com:Yelp/data_pipeline_avro_util.git
```


Tests
-----
Running unit tests
```
make test
```


Usage
-----
Using Avro Schema Builder::
```
from data_pipeline_avro_util.avro_builder import AvroSchemaBuilder

avro_builder = AvroSchemaBuilder()
avro_builder.begin_record(
    name="test_name",
    namespace="test_namespace",
    doc="test_doc"
)
avro_builder.add_field(
    name = "key1",
    typ = "string",
    doc="test_doc1"
)
avro_builder.add_field(
    name = "key2",
    typ = "string",
    doc="test_doc2"
)
record_json = avro_builder.end()
print record_json

	 {
	    "type": "record",
	    "namespace": "test_namespace",
	    "name": "test_name",
	    "doc": "test_doc",
	    "fields": [
	        {"type": "string", "doc": "test_doc1", "name": "key1"},
	        {"type": "string", "doc": "test_doc2", "name": "key2"}
	    ]
	}
```


License
-------
Data Pipeline Avro Util is licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0


Contributing
------------
Everyone is encouraged to contribute to Data Pipeline Avro Util by forking the Github repository and making a pull request or opening an issue.
