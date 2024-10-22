package zipkin2.server.internal.clickhouse;

import com.clickhouse.client.api.Client;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import zipkin2.clickhouse.ClickHouseStorage;
import zipkin2.storage.StorageComponent;

import java.sql.SQLException;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.Executor;

@Configuration
@ConditionalOnProperty(name = "zipkin.storage.type", havingValue = "clickhouse")
@EnableConfigurationProperties({ZipkinClickHouseStorageProperties.class})
@ConditionalOnClass({ClickHouseStorage.class})
@ConditionalOnMissingBean({StorageComponent.class})
public class ZipkinClickHouseStorageConfiguration {
  private ZipkinClickHouseStorageProperties clickHouseStorageProperties;

  public ZipkinClickHouseStorageConfiguration(ZipkinClickHouseStorageProperties clickHouseStorageProperties) {
    this.clickHouseStorageProperties = clickHouseStorageProperties;
  }

  @Bean
  @ConditionalOnMissingBean
  public Executor clickHouseExecutor() {
    ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
    executor.setThreadNamePrefix("ZipkinClickHouseStorage-");
    executor.initialize();
    return executor;
  }

  @Bean
  @ConditionalOnMissingBean()
  public Client client () throws SQLException {
    Client.Builder clientBuilder = new Client.Builder()
      .addEndpoint(clickHouseStorageProperties.getEndpoint())
      .setUsername(clickHouseStorageProperties.getUsername())
      .setPassword(clickHouseStorageProperties.getPassword())
      .compressServerResponse(true)
      .setDefaultDatabase(clickHouseStorageProperties.getDatabase())
      .setSocketTimeout(10, ChronoUnit.SECONDS)
      .setSocketRcvbuf(1_000_000)
      .setClientNetworkBufferSize(1_000_000)
      .setMaxConnections(20);
    Client client = clientBuilder.build();
    return client;
  }


  @Bean
  public StorageComponent storage(Executor executor,
    Client client,
    @Value("${zipkin.storage.strict-trace-id:true}") boolean strictTraceId,
    @Value("${zipkin.storage.search-enabled:true}") boolean searchEnabled,
    @Value("${zipkin.storage.autocomplete-keys:}") List<String> autocompleteKeys) {
    return ClickHouseStorage.newBuilder()
      .executor(executor)
      .client(client)
      .spanTable(clickHouseStorageProperties.getSpanTable())
      .traceTable(clickHouseStorageProperties.getTraceTable())
      .namesLookback(clickHouseStorageProperties.getNamesLookback())
      .batchSize(clickHouseStorageProperties.getBatchSize())
      .parallelWriteSize(clickHouseStorageProperties.getParallelWriteSize())
      .schedulingTime(clickHouseStorageProperties.getSchedulingTime())
      .strictTraceId(strictTraceId)
      .searchEnabled(searchEnabled)
      .autocompleteKeys(autocompleteKeys)
      .build();

  }

}
