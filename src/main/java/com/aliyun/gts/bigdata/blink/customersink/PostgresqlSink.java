package com.aliyun.gts.bigdata.blink.customersink;

import com.alibaba.blink.streaming.connector.custom.api.CustomSinkBase;
import com.alibaba.blink.streaming.connectors.common.MetricUtils;
import com.alibaba.blink.streaming.connectors.common.errcode.ConnectorErrors;
import com.alibaba.blink.streaming.connectors.common.exception.BlinkRuntimeException;
import com.alibaba.blink.streaming.connectors.common.exception.ErrorUtils;
import com.alibaba.blink.streaming.connectors.common.output.TupleRichOutputFormat;
import com.alibaba.blink.streaming.connectors.common.source.parse.DirtyDataStrategy;
import com.alibaba.blink.streaming.connectors.common.util.ConnectionPool;
import com.alibaba.blink.streaming.connectors.common.util.SQLExceptionSkipPolicy;
import com.alibaba.blink.streaming.connectors.common.util.TpsLimitUtils;
import com.alibaba.druid.pool.DruidDataSource;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import com.alibaba.blink.streaming.connectors.common.util.JdbcUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.shaded.guava18.com.google.common.base.Joiner;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.*;

public class PostgresqlSink extends CustomSinkBase {
    private static Logger LOG = LoggerFactory.getLogger(PostgresqlSink.class);

    // Postgres数据库的IP地址或者Hostname
    private String hostname;
    // Postgres数据库服务的端口号
    private int port;
    // Postgres数据库服务的用户名
    private String username;
    // Postgres数据库服务的密码
    private String password;
    // Postgres 数据库名称
    private String dbName;
    // Postgres Schema名称
    private String schemaName;
    // Postgres 表名
    private String tableName;

    private Connection dbConn;
    private PreparedStatement upload;

    private int batchCount = 0;

    private int[] typesArray;

    private transient DruidDataSource dataSource;
    private static ConnectionPool<DruidDataSource> dataSourcePool = new ConnectionPool<>();
    //
    private String driverClassName = "org.postgresql.Driver";
    private String dataSourceKey = "";
    private int connectionMaxActive = 40;
    private int connectionInitialSize = 1;
    private int connectionMinIdle = 0;
    private boolean connectionTestWhileIdle = false;
    private int maxWait = 15000;
    private int removeAbandonedTimeout = 60 * 10;
    private int batchSize = 50;
    private int bufferSize = 500;
    private List<String> exceptUpdateKeys = new ArrayList<>();
    private long flushIntervalMs = 5000;
    private long currentCount = 0;
    private long maxSinkTps = -1;
    private int numTasks = 1;
    private final Map<String, Tuple2<Boolean,Row>> mapReduceBuffer = new HashMap<>();
    private List<Row> writeAddBuffer;
    private int curAddCount = 0;
    private List<Row> writeDelBuffer;
    private int curDelCount = 0;
    private transient Connection connection;
    private transient Statement statement;
    private boolean ignoreDelete = false;

    private volatile transient Exception flushException = null;
    private volatile boolean flushError = false;

    private String fieldNames = null;

    private boolean skipDuplicateEntryError = true;
    private boolean existsPrimaryKeys = true;
    private DirtyDataStrategy dirtyDataStrategy = DirtyDataStrategy.EXCEPTION;

    private Meter outTps;
    private Meter outBps;
    private Counter sinkSkipCounter;
    private MetricUtils.LatencyGauge latencyGauge;

    private List<String> pkFields = null;
    private int maxRetryTime = 3;

    private final String INSERT_SQL_TPL = "INSERT INTO %s (%s) VALUES (%s)";

    private final String DELETE_WITH_KEY_SQL_TPL = "DELETE FROM %s WHERE %s LIMIT 1";

    private final String CREATE_TABLE_SQL_TPL = "CREATE TABLE IF NOT EXISTS %s (%s)";

    private transient Configuration config;
    private transient Timer flusher;
    private volatile long lastFlushTime = 0;
    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
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

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getDriverClassName() {
        return driverClassName;
    }

    public void setDriverClassName(String driverClassName) {
        this.driverClassName = driverClassName;
    }

    public String getDataSourceKey() {
        return dataSourceKey;
    }

    public void setDataSourceKey(String dataSourceKey) {
        this.dataSourceKey = dataSourceKey;
    }

    public void configure(Configuration configuration) {
        this.config = configuration;
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        this.numTasks = numTasks;
        LOG.info(String.format("Open Method Called: taskNumber %d numTasks %d", taskNumber, numTasks));
        String[] filedNames = rowTypeInfo.getFieldNames();
        TypeInformation[] typeInformations = rowTypeInfo.getFieldTypes();
        LOG.info(String.format("Open Method Called: filedNames %d typeInformations %d", filedNames.length,
                typeInformations.length));

        this.setHostname(userParamsMap.get("hostname"));
        this.setPort(Integer.valueOf(userParamsMap.get("port")));
        this.setUsername(userParamsMap.get("username"));
        this.setPassword(userParamsMap.get("password"));
        this.setDbName(userParamsMap.containsKey("db_name")?userParamsMap.get("db_name") : "postgres");
        this.setSchemaName(userParamsMap.containsKey("schema_name")?userParamsMap.get("schema_name") : "public");
        this.setTableName(userParamsMap.get("table_name"));
        // this.maxWait = maxWait;
        // this.removeAbandonedTimeout = removeAbandonedTimeout;

        Joiner joinerOnComma = Joiner.on(",").useForNull("null");
        String[] fieldNamesStr = new String[rowTypeInfo.getArity()];
        for (int i = 0; i < fieldNamesStr.length; i++) {
            // fieldNamesStr[i] = "\"" + rowTypeInfo.getFieldNames()[i] + "\"";
            // postgresql column name 不需要分隔符
            fieldNamesStr[i] = rowTypeInfo.getFieldNames()[i];
        }
        this.fieldNames = joinerOnComma.join(fieldNamesStr);

        if (null != pkFields && !pkFields.isEmpty()){
            existsPrimaryKeys = true;
        } else {
            existsPrimaryKeys = false;
        }

        // 以上为初始化属性

        synchronized (PostgresqlSink.class) {
            this.setDataSourceKey(this.getUrl() + this.getUsername() + this.getPassword() + this.getSchemaName() + this.getTableName());
            if (dataSourcePool.contains(dataSourceKey)){
                dataSource = dataSourcePool.get(dataSourceKey);
            } else {
                dataSource = new DruidDataSource();
                dataSource.setUrl(this.getUrl());
                dataSource.setUsername(this.getUsername());
                dataSource.setPassword(this.getPassword());
                dataSource.setDriverClassName(this.getDriverClassName()); // com.***.***.**.driver
                dataSource.setMaxActive(connectionMaxActive);
                dataSource.setInitialSize(connectionInitialSize);
                dataSource.setMaxWait(maxWait);//默认为15s
                dataSource.setMinIdle(connectionMinIdle);
                dataSource.setTestWhileIdle(connectionTestWhileIdle);
                dataSource.setRemoveAbandoned(false);
                dataSourcePool.put(this.getDataSourceKey(), dataSource);
            }
            try {
                connection = dataSource.getConnection();
                statement = connection.createStatement();
            } catch (Exception e) {
                LOG.error("Error When Init DataSource", e);
            }
        }

        if(existsPrimaryKeys) {
            scheduleFlusher();
            // 默认会把主键排除掉
            for(String pk:pkFields) {
                if(!exceptUpdateKeys.contains(pk)) {
                    exceptUpdateKeys.add(pk);
                }
            }
        }

//        outTps = MetricUtils.registerOutTps(getRuntimeContext());
//        outBps = MetricUtils.registerOutBps(getRuntimeContext(), "postgresql");
//        latencyGauge = MetricUtils.registerOutLatency(getRuntimeContext());
//        sinkSkipCounter = MetricUtils.registerSinkSkipCounter(getRuntimeContext(),getName());
    }

    private String getUrl() {
        return "jdbc:postgresql://" + this.getHostname() + ":" + this.getPort() + "/" + this.getDbName();
    }

    @Override
    public void close() throws IOException {
        LOG.info(String.format("Close Method Called: postgresql sink closed."));

        if (flusher != null) {
            flusher.cancel();
            flusher = null;
        }

        sync();
        try {
            if (null != connection && !connection.isClosed()) {
                connection.close();
            }
        } catch (Exception e) {
            // ignore this exception
        }
        synchronized (PostgresqlSink.class) {
            if (dataSourcePool.remove(dataSourceKey) && dataSource != null && !dataSource.isClosed()) {
                dataSource.close();
                dataSource = null;
            }
        }

    }

    @Override
    public void writeAddRecord(Row row) throws IOException {
        if (flushError && null != flushException){
            throw new RuntimeException(flushException);
        }
        if(!existsPrimaryKeys) {
            // 没有主键直接插入
            String fieldValues = JdbcUtils.toMysqlField(row.getField(0));
            for (int i = 1; i < row.getArity(); i++) {
                fieldValues = fieldValues.concat("," + JdbcUtils.toMysqlField(row.getField(i)));
            }
            String sql = String.format(INSERT_SQL_TPL, this.getSchemaName() + "." + this.getTableName(), this.fieldNames, fieldValues);
            executeSql(sql);
        } else {
            currentCount ++;
            String dupKey = JdbcUtils.constructDupKey(row, rowTypeInfo, pkFields);
            synchronized (mapReduceBuffer) {
                mapReduceBuffer.put(dupKey, new Tuple2<>(true, row));
            }
            if(currentCount >= bufferSize){
                sync();
            }
        }
    }

    @Override
    public void writeDeleteRecord(Row row) throws IOException {
        if (flushError && null != flushException){
            throw new RuntimeException(flushException);
        }
        if(ignoreDelete){
            return;
        }
        if(!existsPrimaryKeys) {
            if (null != row) {
                Joiner joinerOnComma = Joiner.on(" AND ").useForNull("null");
                List<String> sub = new ArrayList<>();
                for (int i = 0; i < row.getArity(); i++) {
                    sub.add(" " + rowTypeInfo.getFieldNames()[i] + " = " +
                            JdbcUtils.toMysqlField(row.getField(i)));
                }
                String sql = String.format(DELETE_WITH_KEY_SQL_TPL, tableName, joinerOnComma.join(sub));
                executeSql(sql);
            }
        } else {
            currentCount ++;
            String dupKey = JdbcUtils.constructDupKey(row, rowTypeInfo, pkFields);
            synchronized (mapReduceBuffer) {
                mapReduceBuffer.put(dupKey, new Tuple2<>(false, row));
            }
            if(currentCount >= bufferSize){
                sync();
            }
        }
    }

    @Override
    public void sync() throws IOException {
        if(!existsPrimaryKeys){
            return;
        } else {
            //使用synchronized关键字保证flush线程执行的时候，不会同时更新mapReduceBuffer的内容
            synchronized (mapReduceBuffer) {
                List<Row> addBuffer = new ArrayList<>();
                List<Row> deleteBuffer = new ArrayList<>();
                for (Tuple2<Boolean, Row> rowTuple2 : mapReduceBuffer.values()) {
                    if (rowTuple2.f0) {
                        addBuffer.add(rowTuple2.f1);
                    } else {
                        deleteBuffer.add(rowTuple2.f1);
                    }
                }
                batchWrite(addBuffer);
                batchDelete(deleteBuffer);
                mapReduceBuffer.clear();
            }
        }
        lastFlushTime = System.currentTimeMillis();
        currentCount = 0;
    }

    @Override
    public String getName() {
        return "PostgresqlSink";
    }

    private void initConnection() {
        SQLException exception = null;
        for (int i=0; i < maxRetryTime; i++) {
            try {
                if (null == connection) {
                    connection = dataSource.getConnection();
                }
                if (connection.isClosed()) {
                    connection = dataSource.getConnection();
                }
                exception = null;
                break;
            } catch (SQLException e) {
                exception = e;
                LOG.warn("get connection failed, retryTimes=" + i, e);
            }
        }

        if (exception != null || connection == null) {
            ErrorUtils.throwException(ConnectorErrors.INST.rdsGetConnectionError("postgresql"), exception);
        }
    }


    protected void scheduleFlusher() {
        flusher = new Timer("PostgresqlSink.buffer.flusher");
        flusher.schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    if(System.currentTimeMillis() - lastFlushTime >= flushIntervalMs){
                        synchronized (this){
                            sync();
                        }
                    }
                } catch (Exception e) {
                    LOG.error("flush buffer to postgresql sink failed", e);
                    flushException = e;
                    flushError = true;
                }
            }
        }, flushIntervalMs, flushIntervalMs);
    }

    private void executeSql(String sql) {
        long start = System.currentTimeMillis();
        int retryTime = 0;
        while (retryTime++ < maxRetryTime) {
            try {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(sql);
                }
                if (connection.isClosed()) {
                    connection = dataSource.getConnection();
                    statement = connection.createStatement();
                }
                statement.execute(sql);
                TpsLimitUtils.limitTps(maxSinkTps, numTasks, start, 1);
                break;
            } catch (SQLException e) {
                LOG.error("Insert into db error,exception:", e);
                if (retryTime == maxRetryTime) {
                    SQLException newE = e;
                    BlinkRuntimeException blinkException = ErrorUtils.getException(
                            ConnectorErrors.INST.rdsWriteError("RDS", sql), newE);
                    LOG.error(blinkException.getErrorMessage() + " sql:" + sql);
                }
                try {
                    // sleep according to retryTimes
                    Thread.sleep(1000 * retryTime);
                } catch (Exception e1) {
                    //ignore
                }
            } finally {
                try {
                    connection.close();
                } catch (Exception e) {
                    LOG.error("", e);
                }
            }
        }

        // report metrics
        long end = System.currentTimeMillis();
//        latencyGauge.report(end - start, 1);
//        outTps.markEvent();
//        outBps.markEvent(sql.length() * 2);
    }

    private void batchWrite(List<Row> buffers) {
        try {
            initConnection();
            for (Row row : buffers) {
                if (writeAddBuffer == null) {
                    writeAddBuffer = new ArrayList<>();
                    this.curAddCount = 0;
                }
                writeAddBuffer.add(row);
                this.curAddCount++;
                if (curAddCount >= batchSize) {
                    execBatchAdd();
                    this.writeAddBuffer = null;
                    this.curAddCount = 0;
                }
            }
            execBatchAdd();
        } finally {
            if (null != connection) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    LOG.error("", e);
                }
            }
        }
    }

    private void execBatchAdd() {
        if( null == writeAddBuffer || writeAddBuffer.isEmpty()) {
            return;
        }

        int count = writeAddBuffer.size();
        int retriedTimes = 0;
        while (!writeAddBuffer.isEmpty() && retriedTimes++ < maxRetryTime) {
            long start = System.currentTimeMillis();
            String sql = JdbcUtils.getDuplicateUpdateSql(rowTypeInfo, exceptUpdateKeys, tableName);
            PreparedStatement preparedStatement = null;
            int sinkCount = 0;
            try {
                connection.setAutoCommit(false);
                preparedStatement = connection.prepareStatement(sql);
                for (Row row : writeAddBuffer) {
                    JdbcUtils.setUpdateStatement(preparedStatement, row, rowTypeInfo);
                    preparedStatement.addBatch();
                    sinkCount++;
                }

                LOG.info("BatchUpdateSize [{}]", count);
                long s1 = System.currentTimeMillis();
                preparedStatement.executeBatch();
                connection.commit();
                if (latencyGauge != null) {
                    long s2 = System.currentTimeMillis();
                    latencyGauge.report(s2 - s1, count);
                }
                if (outTps != null) {
                    outTps.markEvent(count);
                }
                if (outBps != null) {
                    // rough estimate
                    outBps.markEvent(count * 1000);
                }
                TpsLimitUtils.limitTps(maxSinkTps, numTasks, start, sinkCount);

                lastFlushTime = System.currentTimeMillis();
                writeAddBuffer.clear();
                break;
            } catch (SQLException e) {
                LOG.error("Batch write postgresql error, retry times=" + retriedTimes, e);
                if (connection != null) {
                    try {
                        LOG.warn("Transaction is being rolled back");
                        connection.rollback();
                    } catch (Exception ex) {
                        LOG.warn("Rollback failed", ex);
                    }
                }

                if (retriedTimes >= maxRetryTime) {
                    BlinkRuntimeException blinkException = ErrorUtils.getException(
                            ConnectorErrors.INST.rdsBatchWriteError("RDS",sql, String.valueOf(preparedStatement)),e);
                    if (SQLExceptionSkipPolicy.judge(dirtyDataStrategy, e.getErrorCode(), blinkException)) {
                        sinkSkipCounter.inc(count);
                        LOG.error(blinkException.getErrorMessage() + " sql:" + sql);
                    }
                }

                try {
                    Thread.sleep(1000 * retriedTimes);
                } catch (Exception e1) {
                    LOG.error("Thread sleep exception in RdsOutputFormat class", e1);
                }
            } finally {
                if (null != preparedStatement) {
                    try {
                        preparedStatement.close();
                    } catch (SQLException e) {
                        LOG.error("preparedStatement close error", e);
                    }
                }
            }
        }
    }

    private void batchDelete(List<Row> buffers) {
        try {
            initConnection();
            for (Row row : buffers) {
                if (writeDelBuffer == null) {
                    writeDelBuffer = new ArrayList<>();
                    this.curDelCount = 0;
                }
                writeDelBuffer.add(row);
                this.curDelCount++;
                if (curDelCount >= batchSize) {
                    execBatchDelete();
                    this.writeDelBuffer = null;
                    this.curDelCount = 0;
                }
            }
            execBatchDelete();
        } finally {
            if (null != connection) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    LOG.error("", e);
                }
            }
        }
    }

    private void execBatchDelete() {
        if( null == writeDelBuffer || writeDelBuffer.isEmpty()) {
            return;
        }

        int count = writeDelBuffer.size();
        int retriedTimes = 0;
        while (!writeDelBuffer.isEmpty() && retriedTimes++ < maxRetryTime) {
            long start = System.currentTimeMillis();
            String sql = JdbcUtils.getDeleteSql(pkFields, tableName, true);
            PreparedStatement preparedStatement = null;
            int sinkCount = 0;
            try {
                connection.setAutoCommit(false);
                preparedStatement = connection.prepareStatement(sql);
                for (Row row : writeDelBuffer) {
                    JdbcUtils.setDeleteStatement(preparedStatement, row, rowTypeInfo, pkFields);
                    preparedStatement.addBatch();
                    sinkCount++;
                }

                LOG.info("BatchDeleteSize [{}]", count);
                long s1 = System.currentTimeMillis();
                preparedStatement.executeBatch();
                connection.commit();
                /*
                if (latencyGauge != null) {
                    long s2 = System.currentTimeMillis();
                    latencyGauge.report(s2 - s1, count);
                }
                if (outTps != null) {
                    outTps.markEvent(count);
                }
                if (outBps != null) {
                    // rough estimate
                    outBps.markEvent(count * 1000);
                }*/
                TpsLimitUtils.limitTps(maxSinkTps, numTasks, start, sinkCount);

                lastFlushTime = System.currentTimeMillis();
                writeDelBuffer.clear();
                break;
            } catch (SQLException e) {
                LOG.error("Batch delete rds error, retry times=" + retriedTimes, e);
                if (connection != null) {
                    try {
                        LOG.warn("Transaction is being rolled back");
                        connection.rollback();
                    } catch (Exception ex) {
                        LOG.warn("Rollback failed", ex);
                    }
                }

                if (retriedTimes >= maxRetryTime) {
                    BlinkRuntimeException blinkException = ErrorUtils.getException(
                            ConnectorErrors.INST.rdsBatchDeleteError("RDS",sql, String.valueOf(preparedStatement)),e);

                    if (SQLExceptionSkipPolicy.judge(dirtyDataStrategy, e.getErrorCode(), blinkException)) {
                        sinkSkipCounter.inc(count);
                        LOG.error(blinkException.getErrorMessage() + " sql:" + sql);
                    }
                }

                try {
                    Thread.sleep(1000 * retriedTimes);
                } catch (Exception e1) {
                    LOG.error("Thread sleep exception in RdsOutputFormat class", e1);
                }
            } finally {
                if (null != preparedStatement) {
                    try {
                        preparedStatement.close();
                    } catch (SQLException e) {
                        LOG.error("preparedStatement close error", e);
                    }
                }
            }
        }
    }


}
