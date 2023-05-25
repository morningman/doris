// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.datasource.hive;

import org.apache.doris.catalog.HdfsResource;
import org.apache.doris.common.ThreadPoolManager;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Inspired by https://github.com/apache/dolphinscheduler/blob/dev/dolphinscheduler-datasource-plugin/
 * dolphinscheduler-datasource-hive/src/main/java/org/apache/dolphinscheduler/plugin/datasource/hive/
 * security/UserGroupInformationFactory.java
 */
public class UGIManager {
    private static final Logger LOG = LogManager.getLogger(UGIManager.class);

    // user -> reference counter
    private static final Map<String, Integer> loginUserRefCounter = Maps.newHashMap();
    // user -> UserGroupInformation
    private static final Map<String, CloseableUGI> ugis = Maps.newHashMap();

    private static final ScheduledExecutorService kerberosRenewalThread =
            ThreadPoolManager.newDaemonScheduledThreadPool(1, "kerberos-renewal-thread", true);

    static {
        kerberosRenewalThread.scheduleWithFixedDelay(() -> {
            ugis.forEach((user, ugi) -> {
                synchronized (UGIManager.class) {
                    try {
                        if (ugi.getUgi().isFromKeytab()) {
                            ugi.getUgi().checkTGTAndReloginFromKeytab();
                            LOG.info("finished to renew kerberos for user: {}", user);

                        }
                    } catch (Exception e) {
                        LOG.warn("failed to renew kerberos for user: {}", user, e);
                    }
                }
            });
        }, 0, 10, TimeUnit.MINUTES);
    }

    public static synchronized CloseableUGI get(Map<String, String> properties) {
        String user = getUser(properties);
        CloseableUGI ugi = ugis.get(user);
        if (ugi == null) {
            Configuration conf = getConf(properties);
            if (useKerberos(conf)) {
                ugi = createUGIByKerberos(conf);
            } else {
                ugi = createUGIByUser(conf);
            }
            ugis.put(user, ugi);
        }
        loginUserRefCounter.compute(user, (k, v) -> v == null ? 1 : v + 1);
        return ugi;
    }

    private static Configuration getConf(Map<String, String> properties) {
        Configuration conf = new Configuration();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            conf.set(entry.getKey(), entry.getValue());
        }
        return conf;
    }

    private static String getUser(Map<String, String> properties) {
        String user = properties.get(HdfsResource.HADOOP_USER_NAME);
        if (Strings.isNullOrEmpty(user)) {
            user = properties.get(HdfsResource.HADOOP_KERBEROS_PRINCIPAL);
        }
        if (Strings.isNullOrEmpty()) {
            user = "hadoop";
        }
        return user;
    }

    public static synchronized void logout(String user) {
        int counter = loginUserRefCounter.getOrDefault(user, 0);
        if (counter == 0) {
            return;
        }
        if (counter == 1) {
            loginUserRefCounter.remove(user);
            ugis.remove(ugis);
        } else {
            loginUserRefCounter.put(user, counter - 1);
        }
    }

    private static UserGroupInformation createUGIByUser(String user) {
        return UserGroupInformation.createRemoteUser(user);
    }

    private static CloseableUGI createUGIByKerberos(Configuration conf) {
        String krb5Conf = properties.get("java.security.krb5.conf");
        if (!Strings.isNullOrEmpty(krb5Conf)) {
            System.setProperty("java.security.krb5.conf", krb5Conf);
        }

        String keytab = properties.get(HdfsResource.HADOOP_KERBEROS_KEYTAB);
        String principal = properties.get(HdfsResource.HADOOP_KERBEROS_PRINCIPAL);

        Configuration conf = new Configuration();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            conf.set(entry.getKey(), entry.getValue());
        }
        conf.setBoolean("ipc.client.fallback-to-simple-auth-allowed", true);
        conf.set("hadoop.security.authentication", "kerberos");

        try {
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal.trim(),
                    keytab.trim());
            UserGroupInformation.setLoginUser(ugi);
            return ugi;
        } catch (Exception e) {
            throw new RuntimeException("createUserGroupInformation fail. ", e);
        }
    }

    private static boolean useKerberos(Configuration conf) {
        return "kerberos".equals(conf.get(HdfsResource.HADOOP_SECURITY_AUTHENTICATION));
    }
}
