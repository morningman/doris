package org.apache.doris.tablefunction;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.thrift.TFileType;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Map;

public class KafkaTableValuedFunction extends ExternalFileTableValuedFunction {

    public static final Logger LOG = LogManager.getLogger(HdfsTableValuedFunction.class);
    public static final String NAME = "kafka";
    private static final String PROP_URI = "uri";

    public KafkaTableValuedFunction(Map<String, String> properties) throws AnalysisException {
        init(properties);
    }

    private void init(Map<String, String> properties) throws AnalysisException {
        // 1. analyze common properties
        Map<String, String> otherProps = super.parseCommonProperties(properties);
        locationProperties.putAll(otherProps);
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
