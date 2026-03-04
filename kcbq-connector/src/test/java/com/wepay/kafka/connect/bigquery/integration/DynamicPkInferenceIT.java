package com.wepay.kafka.connect.bigquery.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.integration.utils.SchemaRegistryTestUtils;
import com.wepay.kafka.connect.bigquery.integration.utils.TableClearer;
import com.wepay.kafka.connect.bigquery.retrieve.IdentitySchemaRetriever;
import io.confluent.connect.avro.AvroConverter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.storage.Converter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("integration")
public class DynamicPkInferenceIT extends BaseConnectorIT {

  private static final Logger logger = LoggerFactory.getLogger(DynamicPkInferenceIT.class);

  private static final int TASKS_MAX = 1;
  private static SchemaRegistryTestUtils schemaRegistry;
  private static String schemaRegistryUrl;
  private String connectorName;
  private BigQuery bigQuery;
  private Converter keyConverter;
  private Converter valueConverter;

  private Schema keySchema = SchemaBuilder.struct()
      .field("k1", Schema.INT64_SCHEMA)
      .field("k2", Schema.STRING_SCHEMA)
      .build();
  private Schema valueSchema = SchemaBuilder.struct()
      .field("f1", Schema.STRING_SCHEMA)
      .build();

  @BeforeEach
  public void setup(TestInfo testInfo) throws Exception {
    connectorName = testInfo.getDisplayName();

    startConnect();
    bigQuery = newBigQuery();

    schemaRegistry = new SchemaRegistryTestUtils(connect.kafka().bootstrapServers());
    schemaRegistry.start();
    schemaRegistryUrl = schemaRegistry.schemaRegistryUrl();
  }

  @AfterEach
  public void close() throws Exception {
    connect.deleteConnector(connectorName);
    if (schemaRegistry != null) {
      schemaRegistry.stop();
    }
  }

  @Test
  public void testDynamicPkInferenceFlattened() throws Throwable {
    final String topic = suffixedTableOrTopic("test-flattened-pk");
    connect.kafka().createTopic(topic, TASKS_MAX);

    final String table = sanitizedTable(topic);
    TableClearer.clearTables(bigQuery, dataset(), table);

    Map<String, String> props = baseConnectorProps(TASKS_MAX);
    props.put(SinkConnectorConfig.TOPICS_CONFIG, topic);
    props.put(BigQuerySinkConfig.SANITIZE_TOPICS_CONFIG, "true");
    props.put(BigQuerySinkConfig.SCHEMA_RETRIEVER_CONFIG, IdentitySchemaRetriever.class.getName());
    props.put(BigQuerySinkConfig.TABLE_CREATE_CONFIG, "true");
    
    props.put(ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG, AvroConverter.class.getName());
    props.put("key.converter.schema.registry.url", schemaRegistryUrl);
    props.put(ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, AvroConverter.class.getName());
    props.put("value.converter.schema.registry.url", schemaRegistryUrl);
    
    // Enable upsert but DO NOT provide table.primary.key.fields and DO NOT provide kafkaKeyFieldName
    props.put(BigQuerySinkConfig.UPSERT_ENABLED_CONFIG, "true");
    props.put(BigQuerySinkConfig.DELETE_ENABLED_CONFIG, "true");
    props.put(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG, "false");

    connect.configureConnector(connectorName, props);
    waitForConnectorToStart(connectorName, TASKS_MAX);

    initialiseConverters();

    List<List<SchemaAndValue>> records = new ArrayList<>();
    records.add(Arrays.asList(
        new SchemaAndValue(keySchema, new Struct(keySchema).put("k1", 1L).put("k2", "key1")),
        new SchemaAndValue(valueSchema, new Struct(valueSchema).put("f1", "val1"))
    ));

    schemaRegistry.produceRecordsWithKey(keyConverter, valueConverter, records, topic);
    waitForCommittedRecords(connectorName, topic, 1, TASKS_MAX);

    // Verify table creation and PK inference
    TableId tableId = TableId.of(dataset(), table);
    Table bqTable = bigQuery.getTable(tableId);
    assertNotNull(bqTable, "Table should have been created");

    StandardTableDefinition definition = bqTable.getDefinition();
    
    // 1. Verify PK constraint
    assertNotNull(definition.getTableConstraints(), "Table constraints should be present");
    assertNotNull(definition.getTableConstraints().getPrimaryKey(), "Primary key should be present");
    List<String> pkColumns = definition.getTableConstraints().getPrimaryKey().getColumns();
    assertEquals(Arrays.asList("k1", "k2"), pkColumns, "Primary key columns should be inferred from Kafka key schema");

    // 2. Verify Schema contains the flattened fields
    List<String> schemaFieldNames = definition.getSchema().getFields().stream()
        .map(Field::getName)
        .collect(Collectors.toList());
    assertTrue(schemaFieldNames.contains("k1"), "Schema should contain flattened key field k1");
    assertTrue(schemaFieldNames.contains("k2"), "Schema should contain flattened key field k2");
    assertTrue(schemaFieldNames.contains("f1"), "Schema should contain value field f1");

    logger.info("Successfully verified dynamic Primary Key inference and flattening for table {}", tableId);
  }

  private void initialiseConverters() {
    keyConverter = new AvroConverter();
    valueConverter = new AvroConverter();
    keyConverter.configure(Collections.singletonMap("schema.registry.url", schemaRegistryUrl), true);
    valueConverter.configure(Collections.singletonMap("schema.registry.url", schemaRegistryUrl), false);
  }
}
