/*
 * Copyright 2024 Copyright 2022 Aiven Oy and
 * bigquery-connector-for-apache-kafka project contributors
 *
 * This software contains code derived from the Confluent BigQuery
 * Kafka Connector, Copyright Confluent, Inc, which in turn
 * contains code derived from the WePay BigQuery Kafka Connector,
 * Copyright WePay, Inc.
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

package com.wepay.kafka.connect.bigquery;

import static com.wepay.kafka.connect.bigquery.utils.TableNameUtils.destTable;
import static com.wepay.kafka.connect.bigquery.utils.TableNameUtils.intTable;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import com.google.common.annotations.VisibleForTesting;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkTaskConfig;
import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import com.wepay.kafka.connect.bigquery.exception.BigQueryErrorResponses;
import com.wepay.kafka.connect.bigquery.exception.ExpectedInterruptException;
import com.wepay.kafka.connect.bigquery.utils.SleepUtils;
import com.wepay.kafka.connect.bigquery.utils.Time;
import com.wepay.kafka.connect.bigquery.write.batch.KcbqThreadPoolExecutor;
import com.wepay.kafka.connect.bigquery.write.batch.MergeBatches;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MergeQueries {

  public static final String INTERMEDIATE_TABLE_KEY_FIELD_NAME = "key";
  public static final String INTERMEDIATE_TABLE_VALUE_FIELD_NAME = "value";
  public static final String INTERMEDIATE_TABLE_ITERATION_FIELD_NAME = "i";
  public static final String INTERMEDIATE_TABLE_PARTITION_TIME_FIELD_NAME = "partitionTime";
  public static final String INTERMEDIATE_TABLE_BATCH_NUMBER_FIELD = "batchNumber";
  public static final String DESTINATION_TABLE_ALIAS = "dstTableAlias";
  private static final int WAIT_MAX_JITTER = 1000;
  private static final Logger logger = LoggerFactory.getLogger(MergeQueries.class);

  private final String keyFieldName;
  private final String keySource;
  private final boolean insertPartitionTime;
  private final boolean upsertEnabled;
  private final boolean deleteEnabled;
  private final int bigQueryRetry;
  private final long bigQueryRetryWait;
  private final MergeBatches mergeBatches;
  private final ExecutorService executor;
  private final BigQuery bigQuery;
  private final SchemaManager schemaManager;
  private final SinkTaskContext context;
  private final Time time;

  public MergeQueries(BigQuerySinkTaskConfig config,
                      MergeBatches mergeBatches,
                      KcbqThreadPoolExecutor executor,
                      BigQuery bigQuery,
                      SchemaManager schemaManager,
                      SinkTaskContext context) {
    this(
        config.getKafkaKeyFieldName().orElse(""),
        config.getUpsertDeleteKeySource(),
        config.getBoolean(BigQuerySinkConfig.BIGQUERY_PARTITION_DECORATOR_CONFIG),
        config.getBoolean(BigQuerySinkConfig.UPSERT_ENABLED_CONFIG),
        config.getBoolean(BigQuerySinkConfig.DELETE_ENABLED_CONFIG),
        config.getInt(BigQuerySinkConfig.BIGQUERY_RETRY_CONFIG),
        config.getLong(BigQuerySinkConfig.BIGQUERY_RETRY_WAIT_CONFIG),
        mergeBatches,
        executor,
        bigQuery,
        schemaManager,
        context,
        Time.SYSTEM
    );
  }

  @VisibleForTesting
  MergeQueries(String keyFieldName,
               boolean insertPartitionTime,
               boolean upsertEnabled,
               boolean deleteEnabled,
               int bigQueryRetry,
               long bigQueryRetryWait,
               MergeBatches mergeBatches,
               ExecutorService executor,
               BigQuery bigQuery,
               SchemaManager schemaManager,
               SinkTaskContext context,
               Time time) {
    this(keyFieldName, BigQuerySinkConfig.UPSERT_DELETE_KEY_SOURCE_RECORD_KEY, insertPartitionTime, upsertEnabled, deleteEnabled, bigQueryRetry, bigQueryRetryWait, mergeBatches, executor, bigQuery, schemaManager, context, time);
  }

  public MergeQueries(String keyFieldName,
                      String keySource,
                      boolean insertPartitionTime,
                      boolean upsert,
                      boolean delete,
                      int retry,
                      long retryWait,
                      MergeBatches mergeBatches,
                      ExecutorService executor,
                      BigQuery bigQuery,
                      SchemaManager schemaManager,
                      SinkTaskContext context,
                      Time time) {
    this.keyFieldName = keyFieldName;
    this.keySource = keySource;
    this.insertPartitionTime = insertPartitionTime;
    this.upsertEnabled = upsert;
    this.deleteEnabled = delete;
    this.bigQueryRetry = retry;
    this.bigQueryRetryWait = retryWait;
    this.mergeBatches = mergeBatches;
    this.executor = executor;
    this.bigQuery = bigQuery;
    this.schemaManager = schemaManager;
    this.context = context;
    this.time = time;
  }

  // DELETE FROM `<dataset>`.`<intermediateTable>` WHERE batchNumber <= <batchNumber> AND _PARTITIONTIME IS NOT NULL;
  @VisibleForTesting
  static String batchClearQuery(TableId intermediateTable, int batchNumber) {
    StringBuilder sb = new StringBuilder("DELETE FROM ");
    if (intermediateTable.getProject() != null && !intermediateTable.getProject().isEmpty()) {
      sb.append("`").append(intermediateTable.getProject()).append("`.");
    }
    sb.append("`").append(intermediateTable.getDataset()).append("`.`").append(intermediateTable.getTable()).append("` ")
        .append("WHERE ")
        .append(INTERMEDIATE_TABLE_BATCH_NUMBER_FIELD).append(" <= ").append(batchNumber).append(" ")
        // Use this clause to filter out rows that are still in the streaming buffer, which should
        // not be subjected to UPDATE or DELETE operations or the query will FAIL
        .append("AND _PARTITIONTIME IS NOT NULL")
        .append(";");
    return sb.toString();
  }

  private static List<String> listFields(FieldList keyFields, String prefix) {
    return listFields(keyFields, prefix, true);
  }

  private static List<String> listFields(FieldList keyFields, String prefix, boolean recurse) {
    return keyFields.stream()
        .flatMap(field -> {
          String fieldName = prefix + field.getName();
          FieldList subFields = field.getSubFields();
          if (subFields == null || !recurse) {
            return Stream.of(fieldName);
          }
          return listFields(subFields, fieldName + ".", true).stream();
        }).collect(Collectors.toList());
  }

  public void mergeFlushAll() {
    logger.debug("Triggering merge flush for all tables");
    mergeBatches.intermediateTables().forEach(this::mergeFlush);
  }

  public void mergeFlush(TableId intermediateTable) {
    if (mergeBatches.isCurrentBatchEmpty(intermediateTable)) {
      logger.debug("Merge flush is not performed as the current batch is empty.");
      return;
    }
    final TableId destinationTable = mergeBatches.destinationTableFor(intermediateTable);
    final int batchNumber = mergeBatches.incrementBatch(intermediateTable);
    logger.trace("Triggering merge flush from {} to {} for batch {}",
        intTable(intermediateTable), destTable(destinationTable), batchNumber);

    executor.execute(() -> {
      try {
        mergeFlush(intermediateTable, destinationTable, batchNumber);
      } catch (InterruptedException e) {
        throw new ExpectedInterruptException(String.format(
            "Interrupted while performing merge flush of batch %d from %s to %s",
            batchNumber, intTable(intermediateTable), destTable(destinationTable)));
      }
    });
  }

  private void mergeFlush(
      TableId intermediateTable, TableId destinationTable, int batchNumber
  ) throws InterruptedException {
    // If there are rows to flush in this batch, flush them
    if (mergeBatches.prepareToFlush(intermediateTable, batchNumber)) {
      logger.debug("Running merge query on batch {} from {}",
          batchNumber, intTable(intermediateTable));
      String mergeFlushQuery = mergeFlushQuery(intermediateTable, destinationTable, batchNumber);
      logger.trace(mergeFlushQuery);

      int attempt = 0;
      boolean success = false;
      while (!success) {
        try {
          if (attempt > 0) {
            SleepUtils.waitRandomTime(time, this.bigQueryRetryWait, WAIT_MAX_JITTER);
          }
          bigQuery.query(QueryJobConfiguration.of(mergeFlushQuery));
          success = true;
        } catch (BigQueryException e) {
          if (attempt >= bigQueryRetry) {
            throw new BigQueryConnectException("Failed to merge rows to destination table `" + destinationTable + "` within " + attempt
                + "  attempts.", e);
          } else if (BigQueryErrorResponses.isCouldNotSerializeAccessError(e)) {
            logger.warn("Serialize access error while merging from {} to {}, retry attempt {}", intermediateTable, destinationTable, ++attempt);
          } else if (BigQueryErrorResponses.isJobInternalError(e)) {
            logger.warn("Job internal error while merging from {} to {}, retry attempt {}", intermediateTable, destinationTable, ++attempt);
          } else {
            throw e;
          }
        }
      }
      logger.trace("Merge from {} to {} completed",
          intTable(intermediateTable), destTable(destinationTable));

      logger.debug("Recording flush success for batch {} from {}",
          batchNumber, intTable(intermediateTable));
      mergeBatches.recordSuccessfulFlush(intermediateTable, batchNumber);

      // Commit those offsets ASAP
      context.requestCommit();

      logger.info("Completed merge flush of batch {} from {} to {}",
          batchNumber, intTable(intermediateTable), destTable(destinationTable));
    }

    // After, regardless of whether we flushed or not, clean up old batches from the intermediate
    // table. Some rows may be several batches old but still in the table if they were in the
    // streaming buffer during the last purge.
    logger.trace("Clearing batches from {} on back from {}", batchNumber, intTable(intermediateTable));
    String batchClearQuery = batchClearQuery(intermediateTable, batchNumber);
    logger.trace(batchClearQuery);
    bigQuery.query(QueryJobConfiguration.of(batchClearQuery));
  }

  @VisibleForTesting
  String mergeFlushQuery(TableId intermediateTable, TableId destinationTable, int batchNumber) {
    Schema intermediateSchema = schemaManager.cachedSchema(intermediateTable);

    if (upsertEnabled && deleteEnabled) {
      return upsertDeleteMergeFlushQuery(intermediateTable, destinationTable, batchNumber, intermediateSchema);
    } else if (upsertEnabled) {
      return upsertMergeFlushQuery(intermediateTable, destinationTable, batchNumber, intermediateSchema);
    } else if (deleteEnabled) {
      return deleteMergeFlushQuery(intermediateTable, destinationTable, batchNumber, intermediateSchema);
    } else {
      throw new IllegalStateException("At least one of upsert or delete must be enabled for merge flushing to occur.");
    }
  }

  /*
      MERGE `<dataset>`.`<destinationTable>`
      USING (
        SELECT * FROM (
          SELECT ARRAY_AGG(
            x ORDER BY i DESC LIMIT 1
          )[OFFSET(0)] src
          FROM `<dataset>`.`<intermediateTable>` x
          WHERE batchNumber=<batchNumber>
          GROUP BY key.<field>[, key.<field>...]
        )
      )
      ON `<destinationTable>`.<keyField>=src.key
      WHEN MATCHED AND src.value IS NOT NULL
        THEN UPDATE SET `<valueField>`=src.value.<field>[, `<valueField>`=src.value.<field>...]
      WHEN MATCHED AND src.value IS NULL
        THEN DELETE
      WHEN NOT MATCHED AND src.value IS NOT NULL
        THEN INSERT (`<keyField>`, [_PARTITIONTIME, ]`<valueField>`[, `<valueField>`])
        VALUES (
          src.key,
          [CAST(CAST(DATE(src.partitionTime) AS DATE) AS TIMESTAMP),]
          src.value.<field>[, src.value.<field>...]
        );
   */
  private String upsertDeleteMergeFlushQuery(
      TableId intermediateTable, TableId destinationTable, int batchNumber, Schema intermediateSchema
  ) {
    List<String> valueColumns = valueColumns(intermediateSchema);

    final String key = INTERMEDIATE_TABLE_KEY_FIELD_NAME;
    final String i = INTERMEDIATE_TABLE_ITERATION_FIELD_NAME;
    final String value = INTERMEDIATE_TABLE_VALUE_FIELD_NAME;
    final String batch = INTERMEDIATE_TABLE_BATCH_NUMBER_FIELD;

    String onClause;
    List<String> keyFields;
    if (BigQuerySinkConfig.UPSERT_DELETE_KEY_SOURCE_RECORD_VALUE.equals(keySource)) {
      // Use PRIMARY KEY NOT ENFORCED columns from the destination table as the MERGE key.
      // These PK columns are always present in the Kafka record value.
      // Using ALL value fields would be wrong: when a non-key field changes, the ON clause
      // would never match the existing row (src.new_value != dst.old_value), breaking upserts.
      List<String> pkColumns = schemaManager.getPrimaryKeyColumns(destinationTable);
      FieldList valueSubFields =
          intermediateSchema.getFields().get(INTERMEDIATE_TABLE_VALUE_FIELD_NAME).getSubFields();

      if (!pkColumns.isEmpty()) {
        // GROUP BY the PK columns from the value (e.g. value.f1, value.f2)
        keyFields = pkColumns.stream()
            .map(col -> INTERMEDIATE_TABLE_VALUE_FIELD_NAME + "." + col)
            .collect(Collectors.toList());
        // ON clause: dst.pk_col = src.value.pk_col  (for each PK column)
        onClause = pkColumns.stream()
            .map(col -> DESTINATION_TABLE_ALIAS + ".`" + col + "`=src." + value + "." + col)
            .collect(Collectors.joining(" AND "));
      } else {
        // Fallback (no PK constraint defined): use all value fields — note this means
        // rows with changed non-key fields won't match, so this behaves like INSERT-only.
        logger.warn(
            "No PRIMARY KEY NOT ENFORCED constraint found on destination table {}. "
                + "Falling back to using all value fields for the MERGE ON clause in "
                + "record_value key source mode. Upserts will only work if all value fields "
                + "are part of the key. Define a PRIMARY KEY NOT ENFORCED constraint on the "
                + "table for correct upsert behaviour.",
            table(destinationTable)
        );
        keyFields = listFields(valueSubFields, INTERMEDIATE_TABLE_VALUE_FIELD_NAME + ".", false);
        onClause = valueSubFields.stream()
            .map(f -> DESTINATION_TABLE_ALIAS + ".`" + f.getName() + "`=src." + value + "." + f.getName())
            .collect(Collectors.joining(" AND "));
      }
    } else {
      keyFields = listFields(
          intermediateSchema.getFields().get(INTERMEDIATE_TABLE_KEY_FIELD_NAME).getSubFields(),
          INTERMEDIATE_TABLE_KEY_FIELD_NAME + "."
      );
      if (keyFieldName != null && !keyFieldName.isEmpty()) {
        onClause = DESTINATION_TABLE_ALIAS + ".`" + keyFieldName + "`=src." + key;
      } else {
        List<Field> keyFieldsList = intermediateSchema.getFields().get(INTERMEDIATE_TABLE_KEY_FIELD_NAME).getSubFields();
        onClause = keyFieldsList.stream()
            .map(f -> DESTINATION_TABLE_ALIAS + ".`" + f.getName() + "`=src." + key + "." + f.getName())
            .collect(Collectors.joining(" AND "));
      }
    }

    String insertColumns = getInsertColumns(intermediateSchema);
    String insertValues = getInsertValues(intermediateSchema);

    return "MERGE " + table(destinationTable) + " " + DESTINATION_TABLE_ALIAS + " "
        + "USING ("
        + "SELECT * FROM ("
        + "SELECT ARRAY_AGG("
        + "x ORDER BY " + i + " DESC LIMIT 1"
        + ")[OFFSET(0)] src "
        + "FROM " + table(intermediateTable) + " x "
        + "WHERE " + batch + "=" + batchNumber + " "
        + "GROUP BY " + String.join(", ", keyFields)
        + ")"
        + ") "
        + "ON " + onClause + " "
        + "WHEN MATCHED AND src." + value + " IS NOT NULL "
        + "THEN UPDATE SET " + valueColumns.stream().map(col -> DESTINATION_TABLE_ALIAS + ".`" + col + "`=src." + value + "." + col).collect(Collectors.joining(", ")) + " "
        + "WHEN MATCHED AND src." + value + " IS NULL "
        + "THEN DELETE "
        + "WHEN NOT MATCHED AND src." + value + " IS NOT NULL "
        + "THEN INSERT (" + insertColumns + ") "
        + "VALUES (" + insertValues + ");";
  }

  /*
      MERGE `<dataset>`.`<destinationTable>`
      USING (
        SELECT * FROM (
          SELECT ARRAY_AGG(
            x ORDER BY i DESC LIMIT 1
          )[OFFSET(0)] src
          FROM `<dataset>`.`<intermediateTable>` x
          WHERE batchNumber=<batchNumber>
          GROUP BY key.<field>[, key.<field>...]
        )
      )
      ON `<destinationTable>`.<keyField>=src.key
      WHEN MATCHED
        THEN UPDATE SET `<valueField>`=src.value.<field>[, `<valueField>`=src.value.<field>...]
      WHEN NOT MATCHED
        THEN INSERT (`<keyField>`, [_PARTITIONTIME, ]`<valueField>`[, `<valueField>`])
        VALUES (
          src.key,
          [CAST(CAST(DATE(src.partitionTime) AS DATE) AS TIMESTAMP),]
          src.value.<field>[, src.value.<field>...]
        );
   */
  private String upsertMergeFlushQuery(
      TableId intermediateTable, TableId destinationTable, int batchNumber, Schema intermediateSchema
  ) {
    List<String> valueColumns = valueColumns(intermediateSchema);

    final String key = INTERMEDIATE_TABLE_KEY_FIELD_NAME;
    final String i = INTERMEDIATE_TABLE_ITERATION_FIELD_NAME;
    final String value = INTERMEDIATE_TABLE_VALUE_FIELD_NAME;
    final String batch = INTERMEDIATE_TABLE_BATCH_NUMBER_FIELD;

    String onClause;
    List<String> keyFields;
    if (BigQuerySinkConfig.UPSERT_DELETE_KEY_SOURCE_RECORD_VALUE.equals(keySource)) {
      keyFields = listFields(
          intermediateSchema.getFields().get(INTERMEDIATE_TABLE_VALUE_FIELD_NAME).getSubFields(),
          INTERMEDIATE_TABLE_VALUE_FIELD_NAME + ".",
          false
      );
      onClause = intermediateSchema.getFields().get(INTERMEDIATE_TABLE_VALUE_FIELD_NAME).getSubFields().stream()
          .map(f -> DESTINATION_TABLE_ALIAS + ".`" + f.getName() + "`=src." + value + "." + f.getName())
          .collect(Collectors.joining(" AND "));
    } else {
      keyFields = listFields(
          intermediateSchema.getFields().get(INTERMEDIATE_TABLE_KEY_FIELD_NAME).getSubFields(),
          INTERMEDIATE_TABLE_KEY_FIELD_NAME + "."
      );
      if (keyFieldName != null && !keyFieldName.isEmpty()) {
        onClause = DESTINATION_TABLE_ALIAS + ".`" + keyFieldName + "`=src." + key;
      } else {
        List<Field> keyFieldsList = intermediateSchema.getFields().get(INTERMEDIATE_TABLE_KEY_FIELD_NAME).getSubFields();
        onClause = keyFieldsList.stream()
            .map(f -> DESTINATION_TABLE_ALIAS + ".`" + f.getName() + "`=src." + key + "." + f.getName())
            .collect(Collectors.joining(" AND "));
      }
    }

    String insertColumns = getInsertColumns(intermediateSchema);
    String insertValues = getInsertValues(intermediateSchema);

    return "MERGE " + table(destinationTable) + " " + DESTINATION_TABLE_ALIAS + " "
        + "USING ("
        + "SELECT * FROM ("
        + "SELECT ARRAY_AGG("
        + "x ORDER BY " + i + " DESC LIMIT 1"
        + ")[OFFSET(0)] src "
        + "FROM " + table(intermediateTable) + " x "
        + "WHERE " + batch + "=" + batchNumber + " "
        + "GROUP BY " + String.join(", ", keyFields)
        + ")"
        + ") "
        + "ON " + onClause + " "
        + "WHEN MATCHED "
        + "THEN UPDATE SET " + valueColumns.stream().map(col -> DESTINATION_TABLE_ALIAS + ".`" + col + "`=src." + value + "." + col).collect(Collectors.joining(", ")) + " "
        + "WHEN NOT MATCHED "
        + "THEN INSERT (" + insertColumns + ") "
        + "VALUES (" + insertValues + ");";
  }

  /*
        Delete-only is the trickiest mode. Naively, we could just run a MERGE using the intermediate
      table as a source and sort in ascending order of iteration. However, this would miss an edge
      case where, for a given key, a non-tombstone record is sent and then followed by a tombstone,
      and would result in all rows with that key being deleted from the table, followed by an
      insertion of a row for the initial non-tombstone record. This is incorrect; any and all
      records with a given key that precede a tombstone should either never make it into BigQuery or
      be deleted once the tombstone record is merge flushed.
        So instead, we have to try to filter out rows from the source (i.e., intermediate) table
      that precede tombstone records for their keys. We do this by:
        - Finding the latest tombstone row for each key in the current batch and extracting the
          iteration number for each, referring to this as the "deletes" table
        - Joining that with the current batch from the intermediate table on the row key, keeping
          both tables' iteration numbers (a RIGHT JOIN is used so that rows whose keys don't have
          any tombstones present are included with a NULL iteration number for the "deletes" table)
        - Filtering out all rows where the "delete" table's iteration number is non-null, and their
          iteration number is less than the "delete" table's iteration number
        This gives us only rows from the most recent tombstone onward, and works in both cases where
      the most recent row for a key is or is not a tombstone.

      MERGE `<dataset>`.`<destinationTable>`
      USING (
        SELECT batch.key AS key, [partitionTime, ]value
         FROM (
          SELECT src.i, src.key FROM (
            SELECT ARRAY_AGG(
              x ORDER BY i DESC LIMIT 1
            )[OFFSET(0)] src
            FROM (
              SELECT * FROM `<dataset>`.`<intermediateTable>`
              WHERE batchNumber=<batchNumber>
            ) x
            WHERE x.value IS NULL
            GROUP BY key.<field>[, key.<field>...])) AS deletes
          RIGHT JOIN (
            SELECT * FROM `<dataset>`.`<intermediateTable`
            WHERE batchNumber=<batchNumber>
          ) AS batch
          USING (key)
        WHERE deletes.i IS NULL OR batch.i >= deletes.i
        ORDER BY batch.i ASC) AS src
      ON `<destinationTable>`.<keyField>=src.key AND src.value IS NULL
      WHEN MATCHED
        THEN DELETE
      WHEN NOT MATCHED AND src.value IS NOT NULL
      THEN INSERT (`<keyField>`, [_PARTITIONTIME, ]`<valueField>`[, `<valueField>`])
      VALUES (
        src.key,
        [CAST(CAST(DATE(src.partitionTime) AS DATE) AS TIMESTAMP),]
        src.value.<field>[, src.value.<field>...]
      );
   */
  private String deleteMergeFlushQuery(
      TableId intermediateTable, TableId destinationTable, int batchNumber, Schema intermediateSchema
  ) {
    final String key = INTERMEDIATE_TABLE_KEY_FIELD_NAME;
    final String i = INTERMEDIATE_TABLE_ITERATION_FIELD_NAME;
    final String value = INTERMEDIATE_TABLE_VALUE_FIELD_NAME;
    final String batch = INTERMEDIATE_TABLE_BATCH_NUMBER_FIELD;

    String onClause;
    List<String> keyFields;
    if (BigQuerySinkConfig.UPSERT_DELETE_KEY_SOURCE_RECORD_VALUE.equals(keySource)) {
      keyFields = listFields(
          intermediateSchema.getFields().get(INTERMEDIATE_TABLE_VALUE_FIELD_NAME).getSubFields(),
          INTERMEDIATE_TABLE_VALUE_FIELD_NAME + ".",
          false
      );
      onClause = intermediateSchema.getFields().get(INTERMEDIATE_TABLE_VALUE_FIELD_NAME).getSubFields().stream()
          .map(f -> DESTINATION_TABLE_ALIAS + ".`" + f.getName() + "`=src." + value + "." + f.getName())
          .collect(Collectors.joining(" AND "));
    } else {
      keyFields = listFields(
          intermediateSchema.getFields().get(INTERMEDIATE_TABLE_KEY_FIELD_NAME).getSubFields(),
          INTERMEDIATE_TABLE_KEY_FIELD_NAME + "."
      );
      if (keyFieldName != null && !keyFieldName.isEmpty()) {
        onClause = DESTINATION_TABLE_ALIAS + ".`" + keyFieldName + "`=src." + key;
      } else {
        List<Field> keyFieldsList = intermediateSchema.getFields().get(INTERMEDIATE_TABLE_KEY_FIELD_NAME).getSubFields();
        onClause = keyFieldsList.stream()
            .map(f -> DESTINATION_TABLE_ALIAS + ".`" + f.getName() + "`=src." + key + "." + f.getName())
            .collect(Collectors.joining(" AND "));
      }
    }

    String insertColumns = getInsertColumns(intermediateSchema);
    String insertValues = getInsertValues(intermediateSchema);

    return "MERGE " + table(destinationTable) + " " + DESTINATION_TABLE_ALIAS + " "
        + "USING ("
        + "SELECT batch." + key + " AS " + key + ", " + partitionTimeColumn() + value + " "
        + "FROM ("
        + "SELECT src." + i + ", src." + key + " FROM ("
        + "SELECT ARRAY_AGG("
        + "x ORDER BY " + i + " DESC LIMIT 1"
        + ")[OFFSET(0)] src "
        + "FROM ("
        + "SELECT * FROM " + table(intermediateTable) + " "
        + "WHERE " + batch + "=" + batchNumber
        + ") x "
        + "WHERE x." + value + " IS NULL "
        + "GROUP BY " + String.join(", ", keyFields) + ")) AS deletes "
        + "RIGHT JOIN ("
        + "SELECT * FROM " + table(intermediateTable) + " "
        + "WHERE " + batch + "=" + batchNumber
        + ") AS batch "
        + "USING (" + key + ") "
        + "WHERE deletes." + i + " IS NULL OR batch." + i + " >= deletes." + i + " "
        + "ORDER BY batch." + i + " ASC) AS src "
        + "ON " + onClause + " AND src." + value + " IS NULL "
        + "WHEN MATCHED "
        + "THEN DELETE "
        + "WHEN NOT MATCHED AND src." + value + " IS NOT NULL "
        + "THEN INSERT (" + insertColumns + ") "
        + "VALUES (" + insertValues + ");";
  }

  private String table(TableId tableId) {
    if (tableId.getProject() != null && !tableId.getProject().isEmpty()) {
      return String.format("`%s`.`%s`.`%s`", tableId.getProject(), tableId.getDataset(), tableId.getTable());
    }
    return String.format("`%s`.`%s`", tableId.getDataset(), tableId.getTable());
  }

  private List<String> valueColumns(Schema intermediateTableSchema) {
    return intermediateTableSchema.getFields().get(INTERMEDIATE_TABLE_VALUE_FIELD_NAME).getSubFields()
        .stream()
        .map(Field::getName)
        .collect(Collectors.toList());
  }

  private String partitionTimePseudoColumn() {
    return insertPartitionTime ? "_PARTITIONTIME, " : "";
  }

  private String partitionTimeValue() {
    return insertPartitionTime
        ? "CAST(CAST(DATE(src." + INTERMEDIATE_TABLE_PARTITION_TIME_FIELD_NAME + ") AS DATE) AS TIMESTAMP), "
        : "";
  }

  private String partitionTimeColumn() {
    return insertPartitionTime
        ? INTERMEDIATE_TABLE_PARTITION_TIME_FIELD_NAME + ", "
        : "";
  }

  private String getInsertColumns(Schema intermediateTableSchema) {
    StringBuilder sb = new StringBuilder();
    if (BigQuerySinkConfig.UPSERT_DELETE_KEY_SOURCE_RECORD_KEY.equals(keySource)) {
      if (keyFieldName != null && !keyFieldName.isEmpty()) {
        sb.append("`").append(keyFieldName).append("`").append(", ");
      } else {
        List<Field> keyFieldsList = intermediateTableSchema.getFields().get(INTERMEDIATE_TABLE_KEY_FIELD_NAME).getSubFields();
        for (Field field : keyFieldsList) {
          sb.append("`").append(field.getName()).append("`").append(", ");
        }
      }
    }
    sb.append(partitionTimePseudoColumn());
    List<String> valueFields = valueColumns(intermediateTableSchema);
    sb.append("`").append(String.join("`, `", valueFields)).append("`");
    return sb.toString();
  }

  private String getInsertValues(Schema intermediateTableSchema) {
    StringBuilder sb = new StringBuilder();
    if (BigQuerySinkConfig.UPSERT_DELETE_KEY_SOURCE_RECORD_KEY.equals(keySource)) {
      if (keyFieldName != null && !keyFieldName.isEmpty()) {
        sb.append("src.key, ");
      } else {
        List<Field> keyFieldsList = intermediateTableSchema.getFields().get(INTERMEDIATE_TABLE_KEY_FIELD_NAME).getSubFields();
        for (Field field : keyFieldsList) {
          sb.append("src.key.").append(field.getName()).append(", ");
        }
      }
    }
    sb.append(partitionTimeValue());
    List<String> valueFields = valueColumns(intermediateTableSchema);
    sb.append(valueFields.stream().map(f -> "src.value." + f).collect(Collectors.joining(", ")));
    return sb.toString();
  }
}
