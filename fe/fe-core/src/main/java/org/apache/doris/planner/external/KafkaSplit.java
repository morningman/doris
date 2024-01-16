package org.apache.doris.planner.external;

import org.apache.hadoop.fs.Path;

import java.util.List;


public class KafkaSplit extends FileSplit {

    private long partition;
    private long startOffset;
    private long maxRows;

    public KafkaSplit(long partition, long startOffset, long maxRows) {
        super(null, 0, 0, 0, 0, null, null);
        this.partition = partition;
        this.startOffset = startOffset;
        this.maxRows = maxRows;
    }

    @Override
    public String[] getHosts() {
        return new String[0];
    }

    @Override
    public Object getInfo() {
        return null;
    }

    public long getPartition() {
        return partition;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public long getMaxRows() {
        return maxRows;
    }
}
