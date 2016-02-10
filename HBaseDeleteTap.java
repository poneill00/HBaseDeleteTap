
package com.dnb.daas.monitoring.common.mr.taps;

import cascading.flow.FlowProcess;
import cascading.hbase.HBaseTap;
import cascading.tap.SinkMode;
import cascading.tuple.TupleEntryCollector;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.OutputFormat;

import java.io.IOException;

public class HBaseDeleteTap extends HBaseTap {

    public HBaseDeleteTap(String tableName, HBaseDeleteScheme scheme) {
        super(tableName, scheme, SinkMode.UPDATE);
    }

    public void sinkConfInit(FlowProcess<JobConf> flowProcess, JobConf conf) {
        conf.setOutputFormat(DeleteTableOutputFormat.class);
        conf.setClass("mapreduce.output.format.class", DeleteTableOutputFormat.class, OutputFormat.class);
        super.sinkConfInit(flowProcess, conf);
    }

    public TupleEntryCollector openForWrite(FlowProcess<JobConf> flowProcess, OutputCollector output) throws IOException {
        HBaseDeleteTapCollector hBaseCollector = new HBaseDeleteTapCollector(flowProcess, this);
        hBaseCollector.prepare();
        return hBaseCollector;
    }
}