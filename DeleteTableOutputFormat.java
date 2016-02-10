/*
 * Copyrighted as an unpublished work 2016 D&B.
 * Proprietary and Confidential.  Use, possession and disclosure subject to license agreement.
 * Unauthorized use, possession or disclosure is a violation of D&B's legal rights and may result
 * in suit or prosecution.
 */
package com.dnb.daas.monitoring.common.mr.taps;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * This class is taken from org.apache.hadoop.hbase.mapred.TableOutputFormat and is identical other than the
 * TableRecordWriter.write() which implements both Put and Delete mutations
 */
public class DeleteTableOutputFormat extends TableOutputFormat {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteTableOutputFormat.class);

    public RecordWriter getRecordWriter(FileSystem ignored, JobConf job, String name, Progressable progress) throws IOException {
        String tableName = job.get("hbase.mapred.outputtable");
        HTable table = null;

        try {
            table = new HTable(HBaseConfiguration.create(job), tableName);

        } catch (IOException e) {
            LOGGER.error("Error: ", e);
            throw e;
        } finally {
            table.close();
        }

        table.setAutoFlush(false, true);
        return new DeleteTableOutputFormat.TableRecordWriter(table);
    }

    protected static class TableRecordWriter implements RecordWriter<ImmutableBytesWritable, Mutation> {

        private HTable table;

        public TableRecordWriter(HTable table) {
            this.table = table;
        }

        public void close(Reporter reporter) throws IOException {
            this.table.close();
        }

        public void write(ImmutableBytesWritable key, Mutation value) throws IOException {
            if (value instanceof Put) {
                this.table.put(new Put((Put) value));
            } else {
                if (!(value instanceof Delete)) {
                    throw new IOException("Pass a Delete or a Put");
                }
                this.table.delete(new Delete((Delete) value));
            }
        }
    }
}

