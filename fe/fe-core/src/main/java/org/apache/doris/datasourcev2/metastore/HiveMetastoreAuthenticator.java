package org.apache.doris.datasourcev2.metastore;

import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.thrift.client.TUGIAssumingTransport;
import org.apache.hadoop.security.UserGroupInformation;
import shade.doris.hive.org.apache.thrift.transport.TSaslClientTransport;
import shade.doris.hive.org.apache.thrift.transport.TTransport;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Locale;
import java.util.Map;
import javax.security.sasl.Sasl;

public class HiveMetastoreAuthenticator {

    private final Configuration conf;

    private String hiveMetastoreServicePrincipal;
    private UserGroupInformation ugi;

    private HiveMetastoreAuthenticator(Configuration conf) {
        this.conf = conf;
        init();
    }

    private void init() {
        hiveMetastoreServicePrincipal = MetastoreConf.getVar(conf, ConfVars.KERBEROS_PRINCIPAL);
        checkArgument(hiveMetastoreServicePrincipal != null,
                "Hive Metastore Kerberos principal is not set");

    }

    public static HiveMetastoreAuthenticator create(Configuration conf) {
        return new HiveMetastoreAuthenticator(conf);
    }

    public TTransport authenticate(TTransport rawTransport, String hiveMetastoreHost)
    {
        try {
            Map<String, String> saslProps = ImmutableMap.of(
                    Sasl.QOP, "auth-conf,auth",
                    Sasl.SERVER_AUTH, "true");

            String[] names = hiveMetastoreServicePrincipal.split("[/@]");
            checkArgument(names.length == 3,
                    "Kerberos principal name does not have the expected hostname part: %s",
                    hiveMetastoreServicePrincipal);
            if (names[1].equals("_HOST")) {
                names[1] = hiveMetastoreHost.toLowerCase(Locale.ENGLISH);
            }

            TTransport saslTransport = new TSaslClientTransport(
                        "GSSAPI", // SaslRpcServer.AuthMethod.KERBEROS
                        null,
                        names[0],
                        names[1],
                        saslProps,
                        null,
                        rawTransport);
            return new TUGIAssumingTransport(saslTransport, ugi);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
