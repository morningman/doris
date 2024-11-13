package org.apache.doris.datasourcev2.metastore;

/**
 * This class implements the caching logic for objects in DorisMetastore.
 * Common levels are:
 *  - CachingDorisMetastore
 *      - TracingDorisMetastore
 *          - HiveDorisMetastore
 *              - HiveMetastore
 */
public class CachingDorisMetastore implements DorisMetastore {

    protected DorisMetastore delegate;

}
