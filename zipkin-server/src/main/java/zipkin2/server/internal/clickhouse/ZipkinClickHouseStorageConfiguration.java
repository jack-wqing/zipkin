package zipkin2.server.internal.clickhouse;

import com.clickhouse.jdbc.ClickHouseDataSource;
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
import java.util.List;
import java.util.Properties;
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
  @ConditionalOnMissingBean
  public ClickHouseDataSource clickHouseDataSource () throws SQLException {
    ClickHouseDataSource dataSource = new ClickHouseDataSource(clickHouseStorageProperties.getUrl() +
      "/" + clickHouseStorageProperties.getDatabase(), clickHouseProperties());
    return dataSource;
  }

  private Properties clickHouseProperties() {
    Properties properties = new Properties();
    return properties;
  }

  @Bean
  public StorageComponent storage(Executor executor,
    ClickHouseDataSource clickHouseDataSource,
    @Value("${zipkin.storage.strict-trace-id:true}") boolean strictTraceId,
    @Value("${zipkin.storage.search-enabled:true}") boolean searchEnabled,
    @Value("${zipkin.storage.autocomplete-keys:}") List<String> autocompleteKeys) {
    return ClickHouseStorage.newBuilder()
      .executor(executor)
      .dataSource(clickHouseDataSource)
      .spanTable(clickHouseStorageProperties.getSpanTable())
      .traceTable(clickHouseStorageProperties.getTraceTable())
      .namesLookback(clickHouseStorageProperties.getNamesLookback())
      .batchSize(clickHouseStorageProperties.getBatchSize())
      .strictTraceId(strictTraceId)
      .searchEnabled(searchEnabled)
      .autocompleteKeys(autocompleteKeys)
      .build();

  }

}
