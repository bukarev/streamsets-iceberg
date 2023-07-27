//@Grab('org.apache.iceberg:iceberg-core:1.1.0')
//@Grab('org.apache.hadoop:hadoop-common:3.3.4')
//@Grab('org.apache.hadoop:hadoop-azure:3.3.4')
//@Grab('org.apache.iceberg:iceberg-parquet:1.1.0')
//@Grab('org.postgresql:postgresql:42.5.1')
//@Grab('org.apache.iceberg:iceberg-hive-metastore:1.1.0')
//@Grab('org.apache.hive:hive-standalone-metastore:3.1.3')
//@GrabExclude('org.apache.logging.log4j:log4j-slf4j-impl')
//@Grab('org.slf4j:slf4j-api:1.7.36')
//@Grab('org.apache.hive:hive-common:3.1.3')
//@Grab('org.apache.hadoop:hadoop-mapreduce-client-core:3.3.4')
//@Grab('org.apache.hive:hive-exec:3.1.3')
//@Grab('org.apache.parquet:parquet-hadoop-bundle:1.12.3')

import java.time.ZoneId;
import org.apache.hadoop.conf.Configuration;

import org.apache.iceberg.Schema;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.iceberg.avro.AvroSchemaUtil;

import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.types.Types;
import org.apache.parquet.schema.Types;
import org.apache.iceberg.types.Type.TypeID;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.AwsProperties;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.data.GenericRecord;

import org.apache.iceberg.Files;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.DataFile;

tableName = sdc.pipelineParameters().get("p_table");
schemaName = sdc.pipelineParameters().get("p_schema");

if(sdc.records.size() > 0) {
 
  Map<String, String> properties = new HashMap<>();
  properties.put(CatalogProperties.CATALOG_IMPL, HiveCatalog.class.getName());
  properties.put(CatalogProperties.URI, "thrift://" + sdc.pipelineParameters().get("p_metastore") + ":9083");
properties.put(CatalogProperties.WAREHOUSE_LOCATION, "s3a://xxxxxxxxxxxxxxxxxxxx/roman/iceberg");
properties.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
properties.put(AwsProperties.S3FILEIO_ENDPOINT, "s3.amazonaws.com");
  
  
  Configuration hadoopConf = new Configuration(); // configs if you use HadoopFileIO
  hadoopConf.set("fs.s3a.access.key", "xxxxxxxxxxxxxxxx");
    hadoopConf.set("fs.s3a.endpoint", "s3.amazonaws.com");
    hadoopConf.set("fs.s3a.secret.key", "xxxxxxxxxxxxxxxxxxx");

  
  HiveCatalog catalog = CatalogUtil.buildIcebergCatalog("hive_prod", properties, hadoopConf);
  TableIdentifier tableId = TableIdentifier.of(schemaName, tableName);

  Table table;
  if (catalog.tableExists(tableId)) {
      table = catalog.loadTable(tableId); 
  }
  else {
      String strSchema = sdc.records[0].attributes['avroSchema'];
      org.apache.avro.Schema avroSchema = org.apache.avro.Schema.parse(strSchema);
      org.apache.iceberg.Schema avroConvertedSchema = AvroSchemaUtil.toIceberg(avroSchema);
      table = catalog.createTable(tableId, avroConvertedSchema, PartitionSpec.unpartitioned());
  }
  icebergSchema = table.schema();

// FILL IN RECORDS
      GenericRecord record;
      ImmutableList.Builder<GenericRecord> builder;
      builder = ImmutableList.builder();
      for (sdcRecord in sdc.records) {
          record = GenericRecord.create(icebergSchema);
          for (String name : sdcRecord.value.keySet()) {
            switch (icebergSchema.findField(name).type().typeId()) {
              case TypeID.LONG:
                 value = sdcRecord.value[name].longValue();
                 break;
              case TypeID.DATE:
                 value = sdcRecord.value[name].toInstant()
                          .atZone(ZoneId.systemDefault())
                          .toLocalDate();
                 break;
              default:
                value = sdcRecord.value[name];
            }
            
            record = record.copy(ImmutableMap.of(name, value));

          }  
          builder.add(record);
      } 
      ImmutableList<GenericRecord> icebergRecords = builder.build();

// WRITE TO FILE

//  HadoopFileIO fileIO = new HadoopFileIO(hadoopConf);
  String fileName = UUID.randomUUID().toString() + ".snappy.parquet";
  OutputFile file = table.io().newOutputFile(table.location() + "/data/" + fileName);
    
  DataWriter<GenericRecord> dataWriter =
     Parquet.writeData(file)
        .schema(icebergSchema)
        .createWriterFunc(GenericParquetWriter.&buildWriter)
        .overwrite()
        .withSpec(PartitionSpec.unpartitioned())
        .build();
  try {
    for (GenericRecord aRecord : icebergRecords) {
        dataWriter.write(aRecord);
    }
  } finally {
      dataWriter.close();
  }

// APPEND DATA FILE TO TABLE
  DataFile dataFile = dataWriter.toDataFile();
  table.newAppend().appendFile(dataFile).commit();
}
