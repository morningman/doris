package org.apache.doris.datasourcev2.metastore;

/**
 * This is an abstract interface of HiveMetastore(or compatible classes),
 * which is used to isolate Hive objects and Doris objects.
 * Most of the interfaces are implemented through HiveMetastore
 * and converted into Doris objects before returning.
 */
public interface DorisMetastore {

}
