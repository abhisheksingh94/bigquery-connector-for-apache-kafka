/*
 * Copyright 2024 Copyright 2022 Aiven Oy and
 * bigquery-connector-for-apache-kafka project contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.wepay.kafka.connect.bigquery.integration;

import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.PrimaryKey;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableConstraints;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.integration.utils.TableClearer;
import com.wepay.kafka.connect.bigquery.retrieve.IdentitySchemaRetriever;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.storage.Converter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

@Tag("integration")
public class VariableKeySourceUpsertIT extends BaseConnectorIT {

  private static final Logger logger = LoggerFactory.getLogger(VariableKeySourceUpsertIT.class);
  private static final long NUM_RECORDS_PRODUCED = 8;
  private static final int TASKS_MAX = 1;

  private String connectorName;
  private BigQuery bigQuery;

  @BeforeEach
  public void setup(TestInfo testInfo) {
    String testMethod = testInfo.getTestMethod()
        .map(Method::getName)
        .orElseThrow(() -> new AssertionError("Test method not found"));
    connectorName = "kcbq-sink-connector-" + testMethod;
    System.err.println("Setting up test: " + testMethod);
    bigQuery = newBigQuery();
    startConnect();
  }

  @AfterEach
  public void close() {
    System.err.println("Tearing down test: " + connectorName);
    bigQuery = null;
    stopConnect();
  }

  private Map<String, String> upsertProps(String keySource, String keyFieldName, long mergeRecordsThreshold) {
    Map<String, String> result = baseConnectorProps(TASKS_MAX);

    // use the JSON converter with schemas enabled
    result.put(KEY_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
    result.put(VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());

    result.put(BigQuerySinkConfig.UPSERT_ENABLED_CONFIG, "true");
    result.put(BigQuerySinkConfig.DELETE_ENABLED_CONFIG, "true");

    result.put(BigQuerySinkConfig.MERGE_INTERVAL_MS_CONFIG, "10000");
    result.put(BigQuerySinkConfig.MERGE_RECORDS_THRESHOLD_CONFIG, Long.toString(mergeRecordsThreshold));

    result.put(BigQuerySinkConfig.UPSERT_DELETE_KEY_SOURCE_CONFIG, keySource);
    result.put(BigQuerySinkConfig.KAFKA_KEY_FIELD_NAME_CONFIG, keyFieldName);

    result.put(BigQuerySinkConfig.SCHEMA_RETRIEVER_CONFIG, IdentitySchemaRetriever.class.getName());
    result.put(BigQuerySinkConfig.TABLE_CREATE_CONFIG, "true");

    return result;
  }

  /**
   * Returns connector props for record_value key source mode.
   * IMPORTANT: does NOT set kafkaKeyFieldName, so the Kafka record key struct fields
   * are NOT added to the BigQuery table schema as REQUIRED columns. In record_value mode,
   * the merge key comes from the record value fields, not from the Kafka key.
   */
  private Map<String, String> upsertPropsRecordValue(long mergeRecordsThreshold) {
    Map<String, String> result = baseConnectorProps(TASKS_MAX);

    // Use StringConverter for the key: in record_value mode the Kafka key is irrelevant for
    // merging, and we produce plain string keys. JsonConverter with schemas.enable=true would
    // fail to deserialize a raw string key.
    result.put(KEY_CONVERTER_CLASS_CONFIG, org.apache.kafka.connect.storage.StringConverter.class.getName());
    result.put(VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());

    result.put(BigQuerySinkConfig.UPSERT_ENABLED_CONFIG, "true");
    result.put(BigQuerySinkConfig.DELETE_ENABLED_CONFIG, "true");

    result.put(BigQuerySinkConfig.MERGE_INTERVAL_MS_CONFIG, "10000");
    result.put(BigQuerySinkConfig.MERGE_RECORDS_THRESHOLD_CONFIG, Long.toString(mergeRecordsThreshold));

    result.put(BigQuerySinkConfig.UPSERT_DELETE_KEY_SOURCE_CONFIG,
        BigQuerySinkConfig.UPSERT_DELETE_KEY_SOURCE_RECORD_VALUE);
    // Intentionally NO kafkaKeyFieldName: in record_value mode the Kafka key is irrelevant
    // for schema construction and should not create extra REQUIRED columns in BigQuery.

    result.put(BigQuerySinkConfig.SCHEMA_RETRIEVER_CONFIG, IdentitySchemaRetriever.class.getName());
    // The table is pre-created by createTableWithPrimaryKey() with the PK constraint.
    // Disable table auto-creation (we manage it) and the partition decorator (the pre-created
    // table is not time-partitioned, so decorator syntax would throw an error).
    result.put(BigQuerySinkConfig.TABLE_CREATE_CONFIG, "false");
    result.put(BigQuerySinkConfig.BIGQUERY_PARTITION_DECORATOR_CONFIG, "false");

    return result;
  }

  /**
   * Helper: pre-creates the BigQuery table with a PRIMARY KEY NOT ENFORCED constraint on 'f1'.
   * This is required for the MERGE ON clause to work correctly in record_value mode,
   * since MergeQueries now uses the BQ PK columns rather than all value fields.
   */
  private void createTableWithPrimaryKey(String dataset, String table) {
    TableId tableId = TableId.of(dataset, table);
    com.google.cloud.bigquery.Schema schema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
        Field.newBuilder("f2", StandardSQLTypeName.FLOAT64).setMode(Field.Mode.NULLABLE).build()
    );
    TableConstraints constraints = TableConstraints.newBuilder()
        .setPrimaryKey(PrimaryKey.newBuilder().setColumns(Collections.singletonList("f1")).build())
        .build();
    TableInfo tableInfo = TableInfo.newBuilder(
        tableId,
        StandardTableDefinition.newBuilder().setSchema(schema).build()
    ).setTableConstraints(constraints).build();
    try {
      bigQuery.create(tableInfo);
      System.err.println("Created table " + table + " with PRIMARY KEY NOT ENFORCED on f1");
    } catch (com.google.cloud.bigquery.BigQueryException e) {
      if (e.getCode() == 409) {
        System.err.println("Table " + table + " already exists, skipping creation");
      } else {
        throw e;
      }
    }
  }

  @Test
  public void testUpsertWithRecordValueKeySource() throws Throwable {
    final String topic = suffixedTableOrTopic("test-upsert-record-value");
    connect.kafka().createTopic(topic, TASKS_MAX);

    final String table = sanitizedTable(topic);
    // Clear then pre-create with PK constraint so MergeQueries can use the PK for ON clause.
    TableClearer.clearTables(bigQuery, dataset(), table);
    createTableWithPrimaryKey(dataset(), table);

    // Connector props: no kafkaKeyFieldName — Kafka key is irrelevant in record_value mode.
    Map<String, String> props = upsertPropsRecordValue(2);
    props.put(SinkConnectorConfig.TOPICS_CONFIG, topic);

    System.err.println("Configuring connector...");
    connect.configureConnector(connectorName, props);
    waitForConnectorToStart(connectorName, TASKS_MAX);
    System.err.println("Connector started.");

    Converter valueConverter = converter(false);

    // Send 8 records in pairs: each pair shares the same f1 (the PK) but has different f2.
    // The MERGE ON clause joins on f1 (from the BQ PRIMARY KEY NOT ENFORCED constraint),
    // so the second record in each pair UPSERTs: f2 from the second record wins.
    //
    //  i=0: f1="0", f2=0.0
    //  i=1: f1="0", f2=2.0   <- upserts i=0, f2 becomes 2.0
    //  i=2: f1="1", f2=4.0
    //  i=3: f1="1", f2=6.0   <- upserts i=2, f2 becomes 6.0  ... etc.
    for (int i = 0; i < NUM_RECORDS_PRODUCED; i++) {
        String kafkaKey = String.valueOf(i); // plain string key; not used for merge
        String kafkaValue = twoFieldValue(valueConverter, topic, i / 2, i * 2.0); // f1=i/2, f2=i*2
        connect.kafka().produce(topic, kafkaKey, kafkaValue);
    }

    waitForCommittedRecords(connectorName, topic, NUM_RECORDS_PRODUCED, TASKS_MAX);

    // Expect 4 unique rows (one per distinct f1), ordered by f1.
    // For each pair, the LAST record wins (higher i -> larger f2):
    //   f1="0" -> f2 = 1*2.0 = 2.0
    //   f1="1" -> f2 = 3*2.0 = 6.0
    //   f1="2" -> f2 = 5*2.0 = 10.0
    //   f1="3" -> f2 = 7*2.0 = 14.0
    List<List<Object>> allRows = readAllRows(bigQuery, table, "f1");
    List<List<Object>> expectedRows = LongStream.range(0, NUM_RECORDS_PRODUCED / 2)
        .mapToObj(i -> Arrays.<Object>asList(
            Long.toString(i),   // f1
            (i * 2 + 1) * 2.0  // f2 from last record with this f1
        ))
        .collect(Collectors.toList());

    assertEquals(expectedRows, allRows);
  }

  @Test
  public void testUpsertWithMultiColumnKeyInference() throws Throwable {
    final String topic = suffixedTableOrTopic("test-upsert-multi-key");
    connect.kafka().createTopic(topic, TASKS_MAX);

    final String table = sanitizedTable(topic);
    TableClearer.clearTables(bigQuery, dataset(), table);

    Map<String, String> props = upsertProps(
        BigQuerySinkConfig.UPSERT_DELETE_KEY_SOURCE_RECORD_KEY,
        "", // Trigger inference
        2
    );
    props.put(SinkConnectorConfig.TOPICS_CONFIG, topic);

    System.err.println("Configuring connector...");
    connect.configureConnector(connectorName, props);
    waitForConnectorToStart(connectorName, TASKS_MAX);
    System.err.println("Connector started.");

    Converter keyConverter = converter(true);
    Converter valueConverter = converter(false);

    // Send records to Kafka
    for (int i = 0; i < NUM_RECORDS_PRODUCED; i++) {
        // Multi-column key (k1, k2)
        String kafkaKey = multiFieldKey(keyConverter, topic, i / 2, i / 2);
        String kafkaValue = simpleValue(valueConverter, topic, i);
        connect.kafka().produce(topic, kafkaKey, kafkaValue);
    }

    waitForCommittedRecords(connectorName, topic, NUM_RECORDS_PRODUCED, TASKS_MAX);

    // readAllRows will return k1, k2, f1. Order by k1, k2.
    List<List<Object>> allRows = readAllRows(bigQuery, table, "k1, k2");
    List<List<Object>> expectedRows = LongStream.range(0, NUM_RECORDS_PRODUCED / 2)
        .mapToObj(i -> Arrays.<Object>asList(
            (double)(i * 2 + 1), // f1
            i, // k1
            i // k2
        ))
        .collect(Collectors.toList());
    
    assertEquals(expectedRows, allRows);
  }

  private Converter converter(boolean isKey) {
    Map<String, Object> props = new HashMap<>();
    props.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, true);
    Converter result = new JsonConverter();
    result.configure(props, isKey);
    return result;
  }

  private String multiFieldKey(Converter converter, String topic, long k1, long k2) {
    final org.apache.kafka.connect.data.Schema schema = SchemaBuilder.struct()
        .field("k1", org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
        .field("k2", org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
        .build();
    final Struct struct = new Struct(schema)
        .put("k1", k1)
        .put("k2", k2);
    return new String(converter.fromConnectData(topic, schema, struct));
  }

  /**
   * Produces a two-field value with 'f1' (STRING, the PK) and 'f2' (FLOAT64, a non-key field).
   * Used by testUpsertWithRecordValueKeySource to verify that upserts correctly match on the
   * PK column (f1) and overwrite only the non-key field (f2).
   */
  private String twoFieldValue(Converter converter, String topic, long f1Val, double f2Val) {
    final org.apache.kafka.connect.data.Schema schema = SchemaBuilder.struct()
        .field("f1", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
        .field("f2", org.apache.kafka.connect.data.Schema.FLOAT64_SCHEMA)
        .build();
    final Struct struct = new Struct(schema)
        .put("f1", Long.toString(f1Val))
        .put("f2", f2Val);
    return new String(converter.fromConnectData(topic, schema, struct));
  }

  private String simpleValue(Converter converter, String topic, long val) {
    final org.apache.kafka.connect.data.Schema schema = SchemaBuilder.struct()
        .field("f1", org.apache.kafka.connect.data.Schema.FLOAT64_SCHEMA)
        .build();
    final Struct struct = new Struct(schema)
        .put("f1", (double)val);
    return new String(converter.fromConnectData(topic, schema, struct));
  }
}
