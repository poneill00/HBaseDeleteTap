import cascading.CascadingException;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tuple.TupleEntrySchemeCollector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HBaseDeleteTapCollector extends TupleEntrySchemeCollector implements OutputCollector {

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseDeleteTapCollector.class);
    private final JobConf conf;
    private final FlowProcess<JobConf> flowProcess;
    private final Tap<JobConf, RecordReader, OutputCollector> tap;
    private final Reporter reporter;
    private RecordWriter writer;

    public HBaseDeleteTapCollector(FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap) throws IOException {
        super(flowProcess, tap.getScheme());
        this.reporter = Reporter.NULL;
        this.flowProcess = flowProcess;
        this.tap = tap;
        this.conf = new JobConf((Configuration) flowProcess.getConfigCopy());
        this.setOutput(this);
    }

    public void prepare() {
        try {
            this.initialize();
        } catch (IOException var2) {
            throw new CascadingException(var2);
        }

        super.prepare();
    }

    private void initialize() throws IOException {
        this.tap.sinkConfInit(this.flowProcess, this.conf);
        OutputFormat outputFormat = this.conf.getOutputFormat();
        LOGGER.debug("Output format class is: " + outputFormat.getClass().toString());
        this.writer = outputFormat.getRecordWriter((FileSystem) null, this.conf, this.tap.getIdentifier(), Reporter.NULL);
        this.sinkCall.setOutput(this);

        outputFormat = new DeleteTableOutputFormat();
        LOGGER.debug("Output format class is: " + outputFormat.getClass().toString());
        this.writer = outputFormat.getRecordWriter((FileSystem) null, this.conf, this.tap.getIdentifier(), Reporter.NULL);
        this.sinkCall.setOutput(this);
    }

    public void close() {
        try {
            LOGGER.debug("closing tap collector for: {}", this.tap);
            this.writer.close(this.reporter);
        } catch (IOException ioe) {
            LOGGER.warn("exception closing: {}", ioe);
            throw new TapException("exception closing HBaseTapCollector", ioe);
        } finally {
            super.close();
        }

    }

    public void collect(Object writableComparable, Object writable) throws IOException {
        if (this.flowProcess instanceof HadoopFlowProcess) {
            ((HadoopFlowProcess) this.flowProcess).getReporter().progress();
        }
        this.writer.write(writableComparable, writable);
    }
}
