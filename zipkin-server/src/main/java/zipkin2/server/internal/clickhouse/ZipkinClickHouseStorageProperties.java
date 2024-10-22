package zipkin2.server.internal.clickhouse;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.io.Serializable;

@ConfigurationProperties(prefix = "zipkin.storage.clickhouse")
public class ZipkinClickHouseStorageProperties implements Serializable {

  private String endpoint = "http://localhost:8123";

  private String username = "default";
  private String password = "";
  private String  database = "log";
  private String spanTable = "zipkin_spans";

  private String traceTable = "zipkin_spans_trace";
  private long namesLookback = 86400000;

  private int batchSize = 100000;

  private int parallelWriteSize = 1;

  private int schedulingTime = 1;

  public String getEndpoint() {
    return endpoint;
  }

  public void setEndpoint(String endpoint) {
    this.endpoint = endpoint;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public String getSpanTable() {
    return spanTable;
  }

  public void setSpanTable(String spanTable) {
    this.spanTable = spanTable;
  }

  public String getTraceTable() {
    return traceTable;
  }

  public void setTraceTable(String traceTable) {
    this.traceTable = traceTable;
  }

  public long getNamesLookback() {
    return namesLookback;
  }

  public void setNamesLookback(long namesLookback) {
    this.namesLookback = namesLookback;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  public int getParallelWriteSize() {
    return parallelWriteSize;
  }

  public void setParallelWriteSize(int parallelWriteSize) {
    this.parallelWriteSize = parallelWriteSize;
  }

  public int getSchedulingTime() {
    return schedulingTime;
  }

  public void setSchedulingTime(int schedulingTime) {
    this.schedulingTime = schedulingTime;
  }
}
