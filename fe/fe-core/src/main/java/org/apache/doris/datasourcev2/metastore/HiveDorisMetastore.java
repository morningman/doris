package org.apache.doris.datasourcev2.metastore;

import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.common.security.authentication.HadoopAuthenticator;
import org.apache.doris.datasource.DatabaseMetadata;
import org.apache.doris.datasource.TableMetadata;
import org.apache.doris.datasource.hive.HivePartitionStatistics;
import org.apache.doris.datasource.hive.HivePartitionWithStatistics;
import org.apache.doris.datasource.hive.event.MetastoreNotificationFetchException;

import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * This is a wrapper class for HiveMetastore.
 * equals to HiveMetadataOps in the old code.
 */
public class HiveDorisMetastore implements DorisMetastore {
    protected HiveMetastore delegate;

    public DatabaseIf getDatabase(String dbName) {

    }

    List<String> getAllDatabases();

    List<String> getAllTables(String dbName);

    boolean tableExists(String dbName, String tblName);

    List<String> listPartitionNames(String dbName, String tblName);

    List<Partition> listPartitions(String dbName, String tblName);

    List<String> listPartitionNames(String dbName, String tblName, long maxListPartitionNum);

    Partition getPartition(String dbName, String tblName, List<String> partitionValues);

    List<Partition> getPartitions(String dbName, String tblName, List<String> partitionNames);

    Table getTable(String dbName, String tblName);

    List<FieldSchema> getSchema(String dbName, String tblName);

    Map<String, String> getDefaultColumnValues(String dbName, String tblName);

    List<ColumnStatisticsObj> getTableColumnStatistics(String dbName, String tblName,
            List<String> columns);

    Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(
            String dbName, String tblName, List<String> partNames, List<String> columns);

    CurrentNotificationEventId getCurrentNotificationEventId();

    NotificationEventResponse getNextNotification(long lastEventId,
            int maxEvents,
            IMetaStoreClient.NotificationFilter filter) throws MetastoreNotificationFetchException;

    long openTxn(String user);

    void commitTxn(long txnId);

    ValidWriteIdList getValidWriteIds(String fullTableName, long currentTransactionId);

    void acquireSharedLock(String queryId, long txnId, String user, TableName tblName,
            List<String> partitionNames, long timeoutMs);

    String getCatalogLocation(String catalogName);

    void createDatabase(DatabaseMetadata catalogDatabase);

    void dropDatabase(String dbName);

    void dropTable(String dbName, String tableName);

    void truncateTable(String dbName, String tblName, List<String> partitions);

    void createTable(TableMetadata catalogTable, boolean ignoreIfExists);

    void updateTableStatistics(
            String dbName,
            String tableName,
            Function<HivePartitionStatistics, HivePartitionStatistics> update);

    void updatePartitionStatistics(
            String dbName,
            String tableName,
            String partitionName,
            Function<HivePartitionStatistics, HivePartitionStatistics> update);

    void addPartitions(String dbName, String tableName, List<HivePartitionWithStatistics> partitions);

    void dropPartition(String dbName, String tableName, List<String> partitionValues, boolean deleteData);

    default void setHadoopAuthenticator(HadoopAuthenticator hadoopAuthenticator) {
        // Ignored by default
    }

    /**
     * close the connection, eg, to hms
     */
    void close();
}
