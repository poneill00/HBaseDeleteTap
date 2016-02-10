
package com.dnb.daas.monitoring.common.mr.taps;

import cascading.flow.FlowProcess;
import cascading.hbase.HBaseScheme;
import cascading.scheme.SinkCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;

import java.io.IOException;

public class HBaseDeleteScheme extends HBaseScheme {

    public HBaseDeleteScheme(Fields keyFields) {
        super(keyFields, null, new Fields());
    }

    protected Delete sinkGetDelete(TupleEntry tupleEntry) {
        Tuple keyTuple = tupleEntry.selectTuple(this.keyField);
        byte[] keyBytes = Bytes.toBytes(keyTuple.getString(0));
        return new Delete(keyBytes);
    }

    public void sink(FlowProcess<JobConf> flowProcess, SinkCall<Object[], OutputCollector> sinkCall) throws IOException {

        TupleEntry tupleEntry = sinkCall.getOutgoingEntry();

        Delete delete = this.sinkGetDelete(tupleEntry);

        OutputCollector output = sinkCall.getOutput();
        output.collect(null, delete);
    }
}
