package zipkin2.clickhouse.spanconsumer;

import com.clickhouse.jdbc.ClickHouseConnection;
import com.clickhouse.jdbc.ClickHouseDataSource;
import com.clickhouse.jdbc.ClickHouseStatement;
import org.apache.commons.lang3.StringUtils;
import zipkin2.Call;
import zipkin2.Span;
import zipkin2.clickhouse.Constants;

import java.sql.SQLException;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ExecuteWriteExecutor{
  private static final Logger logger = Logger.getLogger(ExecuteWriteExecutor.class.getName());
  final ClickHouseDataSource dataSource;
  final String spanTable;
  final Span[] data;
  public ExecuteWriteExecutor(ClickHouseDataSource dataSource, String spanTable, Span[] data) {
    this.dataSource = dataSource;
    this.spanTable = spanTable;
    this.data = data;
  }
  public void execute() {
    if (Objects.isNull(data)) {
      return;
    }
    BatchSql batchSql = new BatchSql(String.format(Constants.INSERT_SQL, spanTable));
    String generateSql = batchSql.generate(data);
    if (StringUtils.isBlank(generateSql)) {
      return;
    }
    try (ClickHouseConnection connection = dataSource.getConnection();
      ClickHouseStatement statement = connection.createStatement()) {
      statement.execute(generateSql);
    } catch (SQLException e) {
      Call.propagateIfFatal(e);
      logger.log(Level.WARNING, "insert span error!", e);
    }
  }
}
