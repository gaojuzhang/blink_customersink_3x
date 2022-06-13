package com.aliyun.gts.bigdata.blink.customersink;

import com.alibaba.blink.streaming.connector.custom.api.CustomSinkBase;
import com.alibaba.blink.streaming.connectors.common.errcode.ConnectorErrors;
import com.alibaba.blink.streaming.connectors.common.exception.BlinkRuntimeException;
import com.alibaba.blink.streaming.connectors.common.exception.ErrorUtils;
import com.alibaba.blink.streaming.connectors.common.util.ConnectionPool;
import com.alibaba.druid.pool.DruidDataSource;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import com.alibaba.blink.streaming.connectors.common.util.JdbcUtils;
import org.apache.flink.shaded.guava18.com.google.common.base.Joiner;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.List;

public class PostgresqlSink extends CustomSinkBase {
    private static Logger LOG = LoggerFactory.getLogger(PostgresqlSink.class);

    private int batchSize;
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
    private transient Connection connection;
    private transient Statement statement;
    private boolean ignoreDelete = false;

    private volatile transient Exception flushException = null;
    private volatile boolean flushError = false;

    private String fieldNames = null;

    private boolean skipDuplicateEntryError = true;
    private boolean existsPrimaryKeys = true;

    private List<String> pkFields = null;
    private int maxRetryTime = 3;

    private final String INSERT_SQL_TPL = "INSERT INTO %s (%s) VALUES (%s)";

    private final String DELETE_WITH_KEY_SQL_TPL = "DELETE FROM %s WHERE %s LIMIT 1";

    private final String CREATE_TABLE_SQL_TPL = "CREATE TABLE IF NOT EXISTS %s (%s)";

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

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        LOG.info(String.format("Open Method Called: taskNumber %d numTasks %d", taskNumber, numTasks));
        String[] filedNames = rowTypeInfo.getFieldNames();
        TypeInformation[] typeInformations = rowTypeInfo.getFieldTypes();
        LOG.info(String.format("Open Method Called: filedNames %d typeInformations %d", filedNames.length,
                typeInformations.length));

        this.setBatchSize(userParamsMap.containsKey("batchsize")?Integer.valueOf(userParamsMap.get("batchsize")) : 1);
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

    }

    private String getUrl() {
        return "jdbc:postgresql://" + this.getHostname() + ":" + this.getPort() + "/" + this.getDbName();
    }

    @Override
    public void close() throws IOException {
        LOG.info(String.format("Close Method Called: postgresql sink closed."));
        /*
        if (flusher != null) {
            flusher.cancel();
            flusher = null;
        }*/

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
        if(!existsPrimaryKeys) {
            // 没有主键直接插入
            String fieldValues = JdbcUtils.toMysqlField(row.getField(0));
            for (int i = 1; i < row.getArity(); i++) {
                fieldValues = fieldValues.concat("," + JdbcUtils.toMysqlField(row.getField(i)));
            }
            String sql = String.format(INSERT_SQL_TPL, this.getSchemaName() + "." + this.getTableName(), this.fieldNames, fieldValues);
            executeSql(sql);
        } else {
            /*
            currentCount ++;
            String dupKey = JdbcUtils.constructDupKey(row, rowTypeInfo, pkFields);
            synchronized (mapReduceBuffer) {
                mapReduceBuffer.put(dupKey, new Tuple2<>(true, row));
            }
            if(currentCount >= bufferSize){
                sync();
            }
            */

        }
    }

    @Override
    public void writeDeleteRecord(Row row) throws IOException {

    }

    @Override
    public void sync() throws IOException {

    }

    @Override
    public String getName() {
        return "PostgresqlSink";
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
                // TpsLimitUtils.limitTps(maxSinkTps, numTasks, start, 1);
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
        // long end = System.currentTimeMillis();
        // latencyGauge.report(end - start, 1);
        // outTps.markEvent();
        // outBps.markEvent(sql.length() * 2);
    }
}
