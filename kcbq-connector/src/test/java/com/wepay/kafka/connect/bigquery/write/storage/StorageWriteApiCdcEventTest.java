package com.wepay.kafka.connect.bigquery.write.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.bigquery.TableId;
import com.wepay.kafka.connect.bigquery.utils.PartitionedTableId;
import com.wepay.kafka.connect.bigquery.utils.SinkRecordConverter;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class StorageWriteApiCdcEventTest {

  private StorageWriteApiBase mockStreamWriter;
  private PartitionedTableId table;
  private SinkRecordConverter mockRecordConverter;
  private StorageApiBatchModeHandler mockBatchModeHandler;
  private Schema keySchema;
  private Schema valueSchema;

  @BeforeEach
  public void setup() {
    mockStreamWriter = mock(StorageWriteApiBase.class);
    table = new PartitionedTableId.Builder(TableId.of("p", "d", "t")).build();
    mockRecordConverter = mock(SinkRecordConverter.class);
    mockBatchModeHandler = mock(StorageApiBatchModeHandler.class);
    
    keySchema = SchemaBuilder.struct().field("key", Schema.STRING_SCHEMA).build();
    valueSchema = SchemaBuilder.struct().field("name", Schema.STRING_SCHEMA).build();
  }

  @Test
  public void testInsertEvent() {
    StorageWriteApiWriter.Builder builder = new StorageWriteApiWriter.Builder(
        mockStreamWriter, table, mockRecordConverter, mockBatchModeHandler, true, true);
    
    SinkRecord record = createRecord("topic", 100, "k1", "Alice");
    Map<String, Object> regularRow = new HashMap<>();
    regularRow.put("name", "Alice");
    when(mockRecordConverter.getRegularRow(record)).thenReturn(regularRow);
    
    builder.addRow(record, null);
    builder.build().run();
    
    ArgumentCaptor<List<ConvertedRecord>> recordsCaptor = ArgumentCaptor.forClass(List.class);
    verify(mockStreamWriter).initializeAndWriteRecords(eq(table), recordsCaptor.capture(), any());
    
    List<ConvertedRecord> capturedRecords = recordsCaptor.getValue();
    assertEquals(1, capturedRecords.size());
    JSONObject json = capturedRecords.get(0).converted();
    assertEquals("UPSERT", json.getString("_CHANGE_TYPE"));
    assertEquals(100L, json.getLong("_CHANGE_SEQUENCE_NUMBER"));
    assertEquals("Alice", json.getString("name"));
  }

  @Test
  public void testDeleteEvent() {
    StorageWriteApiWriter.Builder builder = new StorageWriteApiWriter.Builder(
        mockStreamWriter, table, mockRecordConverter, mockBatchModeHandler, true, true);
    
    SinkRecord record = createRecord("topic", 101, "k1", null);
    Map<String, Object> regularRow = new HashMap<>();
    // For tombstones, getRegularRow might return empty or key data depending on config, 
    // but convertRecord just adds _CHANGE_TYPE=DELETE
    when(mockRecordConverter.getRegularRow(record)).thenReturn(regularRow);
    
    builder.addRow(record, null);
    builder.build().run();
    
    ArgumentCaptor<List<ConvertedRecord>> recordsCaptor = ArgumentCaptor.forClass(List.class);
    verify(mockStreamWriter).initializeAndWriteRecords(eq(table), recordsCaptor.capture(), any());
    
    List<ConvertedRecord> capturedRecords = recordsCaptor.getValue();
    assertEquals(1, capturedRecords.size());
    JSONObject json = capturedRecords.get(0).converted();
    assertEquals("DELETE", json.getString("_CHANGE_TYPE"));
    assertEquals(101L, json.getLong("_CHANGE_SEQUENCE_NUMBER"));
  }

  @Test
  public void testCdcDisabled() {
    StorageWriteApiWriter.Builder builder = new StorageWriteApiWriter.Builder(
        mockStreamWriter, table, mockRecordConverter, mockBatchModeHandler, false, false);
    
    SinkRecord record = createRecord("topic", 102, "k1", "Bob");
    Map<String, Object> regularRow = new HashMap<>();
    regularRow.put("name", "Bob");
    when(mockRecordConverter.getRegularRow(record)).thenReturn(regularRow);
    
    builder.addRow(record, null);
    builder.build().run();
    
    ArgumentCaptor<List<ConvertedRecord>> recordsCaptor = ArgumentCaptor.forClass(List.class);
    verify(mockStreamWriter).initializeAndWriteRecords(eq(table), recordsCaptor.capture(), any());
    
    List<ConvertedRecord> capturedRecords = recordsCaptor.getValue();
    JSONObject json = capturedRecords.get(0).converted();
    assertEquals(false, json.has("_CHANGE_TYPE"));
    assertEquals(false, json.has("_CHANGE_SEQUENCE_NUMBER"));
    assertEquals("Bob", json.getString("name"));
  }

  private SinkRecord createRecord(String topic, long offset, String keyStr, String name) {
    Object key = new Struct(keySchema).put("key", keyStr);
    Object value = null;
    if (name != null) {
      value = new Struct(valueSchema).put("name", name);
    }
    return new SinkRecord(topic, 0, keySchema, key, valueSchema, value, offset);
  }
}
