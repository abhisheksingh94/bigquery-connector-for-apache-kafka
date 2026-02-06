package com.wepay.kafka.connect.bigquery.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.bigquery.TableId;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkTaskConfig;
import com.wepay.kafka.connect.bigquery.convert.BigQueryRecordConverter;
import com.wepay.kafka.connect.bigquery.convert.RecordConverter;
import com.wepay.kafka.connect.bigquery.utils.PartitionedTableId;
import com.wepay.kafka.connect.bigquery.utils.SinkRecordConverter;
import com.wepay.kafka.connect.bigquery.write.batch.TableWriterBuilder;
import com.wepay.kafka.connect.bigquery.write.storage.ConvertedRecord;
import com.wepay.kafka.connect.bigquery.write.storage.StorageApiBatchModeHandler;
import com.wepay.kafka.connect.bigquery.write.storage.StorageWriteApiBase;
import com.wepay.kafka.connect.bigquery.write.storage.StorageWriteApiWriter;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class StorageWriteApiUpsertDeleteIT {

  Schema keySchema = SchemaBuilder.struct().field("key", Schema.STRING_SCHEMA).build();
  Schema valueSchema = SchemaBuilder.struct()
      .field("id", Schema.INT64_SCHEMA)
      .field("name", Schema.STRING_SCHEMA)
      .build();

  @Test
  public void testUpsertCdcFieldsInjection() {
    StorageWriteApiBase mockStreamWriter = Mockito.mock(StorageWriteApiBase.class);
    BigQuerySinkTaskConfig mockedConfig = Mockito.mock(BigQuerySinkTaskConfig.class);
    // Config: Storage API = true, Upsert = true, Delete = true
    when(mockedConfig.getBoolean(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG)).thenReturn(true);
    when(mockedConfig.getBoolean(BigQuerySinkConfig.UPSERT_ENABLED_CONFIG)).thenReturn(true);
    when(mockedConfig.getBoolean(BigQuerySinkConfig.DELETE_ENABLED_CONFIG)).thenReturn(true);

    RecordConverter<Map<String, Object>> recordConverter = new BigQueryRecordConverter(false, false);
    when(mockedConfig.getRecordConverter()).thenReturn(recordConverter);
    
    SinkRecordConverter sinkRecordConverter = new SinkRecordConverter(mockedConfig, null, null);
    PartitionedTableId table = new PartitionedTableId.Builder(TableId.of("p", "d", "t")).build();
    StorageApiBatchModeHandler batchModeHandler = mock(StorageApiBatchModeHandler.class);
    
    // Explicitly pass true, true for upsert/delete enabled
    TableWriterBuilder builder = new StorageWriteApiWriter.Builder(
        mockStreamWriter, table, sinkRecordConverter, batchModeHandler, true, true);
        
    ArgumentCaptor<List<ConvertedRecord>> recordsCaptor = ArgumentCaptor.forClass(List.class);

    // 1. Insert/Upsert Record
    SinkRecord upsertRecord = createRecord("topic", 100, "123", "Alice");
    builder.addRow(upsertRecord, null);
    
    // 2. Delete Record (tombstone)
    SinkRecord deleteRecord = createRecord("topic", 101, "123", null);
    builder.addRow(deleteRecord, null);
    
    builder.build().run();
    
    verify(mockStreamWriter, times(1))
        .initializeAndWriteRecords(any(PartitionedTableId.class), recordsCaptor.capture(), any());
        
    List<ConvertedRecord> capturedMap = recordsCaptor.getValue();
    assertEquals(2, capturedMap.size());
    
    // Check Upsert
    JSONObject jsonUpsert = capturedMap.get(0).converted();
    assertEquals("UPSERT", jsonUpsert.getString("_CHANGE_TYPE"));
    assertEquals(100L, jsonUpsert.getLong("_CHANGE_SEQUENCE_NUMBER"));
    
    // Check Delete
    JSONObject jsonDelete = capturedMap.get(1).converted();
    assertEquals("DELETE", jsonDelete.getString("_CHANGE_TYPE"));
    assertEquals(101L, jsonDelete.getLong("_CHANGE_SEQUENCE_NUMBER"));
  }

  private SinkRecord createRecord(String topic, long offset, String keyStr, String name) {
    Object key = new Struct(keySchema).put("key", keyStr);
    Object value = null;
    if (name != null) {
      value = new Struct(valueSchema).put("id", 1L).put("name", name);
    }
    return new SinkRecord(topic, 0, keySchema, key, valueSchema, value, offset);
  }
}
