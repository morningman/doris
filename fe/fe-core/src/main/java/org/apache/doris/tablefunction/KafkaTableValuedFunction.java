package org.apache.doris.tablefunction;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KafkaResource;
import org.apache.doris.catalog.Resource;
import org.apache.doris.catalog.Resource.ResourceType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.thrift.TFileType;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Map;

public class KafkaTableValuedFunction extends ExternalFileTableValuedFunction {

    public static final Logger LOG = LogManager.getLogger(HdfsTableValuedFunction.class);
    public static final String NAME = "kafka";
    private static final String KAFKA_RESOURCE = "resource";

    public KafkaTableValuedFunction(Map<String, String> properties) throws AnalysisException {
        init(properties);
    }

    private void init(Map<String, String> properties) throws AnalysisException {
        Map<String, String> otherProps = super.parseCommonProperties(properties);
        if (otherProps.containsKey(KAFKA_RESOURCE)) {
            Resource resource = Env.getCurrentEnv().getResourceMgr().getResource(otherProps.get(KAFKA_RESOURCE));
            if (resource.getType() != ResourceType.KAFKA) {
                throw new AnalysisException("resource type is not kafka");
            }
            KafkaResource kafkaResource = (KafkaResource) resource;
            locationProperties.putAll(kafkaResource.getCopiedProperties());
            locationProperties.put(KAFKA_RESOURCE, kafkaResource.getName());
        } else {
            locationProperties.putAll(otherProps);
        }
    }

    @Override
    public TFileType getTFileType() {
        return TFileType.KAFKA;
    }

    @Override
    public String getFilePath() {
        return null;
    }

    @Override
    public BrokerDesc getBrokerDesc() {
        return null;
    }

    @Override
    public String getTableName() {
        return "KafkaTableValuedFunction";
    }
}
