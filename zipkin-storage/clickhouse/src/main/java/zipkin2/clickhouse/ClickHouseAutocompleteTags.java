package zipkin2.clickhouse;

import zipkin2.Call;
import zipkin2.storage.AutocompleteTags;

import java.util.LinkedHashSet;
import java.util.List;

public class ClickHouseAutocompleteTags implements AutocompleteTags {
  final String spanTable;
  final DataSourceCall.Factory dataSourceCallFactory;
  final boolean enabled;
  private long namesLookback;
  final LinkedHashSet<String> autocompleteKeys;
  final Call<List<String>> keysCall;
  public ClickHouseAutocompleteTags(ClickHouseStorage storage) {
    this.spanTable = storage.spanTable;
    this.dataSourceCallFactory = storage.dataSourceCallFactory;
    enabled = storage.searchEnabled && !storage.autocompleteKeys.isEmpty();
    namesLookback = storage.namesLookback;
    autocompleteKeys = new LinkedHashSet<>(storage.autocompleteKeys);
    keysCall = Call.create(storage.autocompleteKeys);
  }

  @Override
  public Call<List<String>> getKeys() {
    if (!enabled) {
      return Call.emptyList();
    }
    return keysCall.clone();
  }
  @Override
  public Call<List<String>> getValues(String key) {

    if (key == null) {
      throw new NullPointerException("key == null");
    }
    if (key.isEmpty()) {
      throw new IllegalArgumentException("key was empty");
    }
    if (!enabled || !autocompleteKeys.contains(key)) {
      return Call.emptyList();
    }
    DataSourceCall<List<String>> result =
      dataSourceCallFactory.create(new SelectAutocompleteValues(spanTable, key, namesLookback));
    return result;
  }
}
