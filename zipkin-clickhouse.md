## clickhouse-集成说明
### zipkin-lens
```
  1、优化: 在搜索页面,进行trace查询，只会显示当前列表的
     修改 tracesSlice.ts， 在99行后面添加// 添加条件 new Date().getFullYear() < 1 跳过，重新查询traces，重新执行trace查询
```
### zipkin-storage
```
  1、新增clickhouse 存储模块
  2、配置说明:
    clickhouse:
      endpoint: ${CH_ENDPOINT:http://clickhouse.tyc.io:8123}  # clickhouse url
      username: ${CH_USERNAME:default}
      password: ${CH_PASSWORD:}
      database: ${CH_DB:log} # span存储数据库
      # span存储的表明
      spanTable: ${CH_SPAN_TABLE:zipkin_spans_dist}   # 检索库
      traceTable: ${CH_TRACE_TABLE:zipkin_spans_trace_dist_mv} #taces搜索库 建议使用spanTable物化视图
      # 需要服务名的时间窗口 1d
      namesLookback: ${CH_NAMES_LOOK_BACK:86400000}  # 各个筛选项查询回溯时间
      batchSize: ${CH_BATCH_SIZE:100000}   # clichouse 每批次写入的数量
      parallelWriteSize: ${CH_PARALLEL_WRITE_SIZE:2}  # 并行写入的线程
      schedulingTime: ${CH_SCHEDULING_TIME:10} # 如果到达这个时间会，进行写入；而不需要等到batchSize指定的数量
```
