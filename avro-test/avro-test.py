from avro import schema, datafile, io

OUTFILE_NAME = 'sample.avro'

SCHEMA_STR = """{
  "type": "record",
  "name": "sampleAvro",
  "namespace": "AVRO",
  "fields": [
      { "name": "name", "type": "string" },
      { "name": "age", "type": "int" },
      { "name": "address", "type": "string" },
      { "name": "value"  , "type": "long" }
  ]
}"""

SCHEMA = schema.parse(SCHEMA_STR)

def write_avro_file():
    data = {}
    data["name"] = "FOO"
    data["age"] = 19
    data["address"] = "10, PARK AVE"
    data["value"] = 120

    rec_writer = io.DatumWriter(SCHEMA)
    
    df_writer = datafile.DataFileWriter(open(OUTFILE_NAME, 'wb'), rec_writer, writers_schema = SCHEMA, codec = "deflate")

    df_writer.append(data)
    
    df_writer.close()


def read_avro_file():
    rec_reader = io.DatumReader()
    df_reader = datafile.DataFileReader(open(OUTFILE_NAME), rec_reader)

    for record in df_reader:
        print record["name"], record["age"]
        print record["address"], record["value"]


if __name__ == "__main__":
    write_avro_file()
    read_avro_file()

