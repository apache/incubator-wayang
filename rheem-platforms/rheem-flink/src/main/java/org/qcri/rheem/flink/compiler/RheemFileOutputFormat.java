package org.qcri.rheem.flink.compiler;

import org.apache.flink.api.common.io.BlockInfo;
import org.apache.flink.api.common.io.CleanupWhenUnsuccessful;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.common.io.InitializeOnMaster;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.qcri.rheem.core.api.exception.RheemException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.URI;

/**
 * Wrapper for {@link FileOutputFormat}
 */
public class RheemFileOutputFormat<IT> extends FileOutputFormat<IT> implements InitializeOnMaster, CleanupWhenUnsuccessful {

    private static final long serialVersionUID = 1L;

    // --------------------------------------------------------------------------------------------

    private static FileSystem.WriteMode DEFAULT_WRITE_MODE;

    private static FileOutputFormat.OutputDirectoryMode DEFAULT_OUTPUT_DIRECTORY_MODE;

    static {
        initDefaultsFromConfiguration(GlobalConfiguration.loadConfiguration());
    }

    /**
     * Initialize defaults for output format. Needs to be a static method because it is configured for local
     * cluster execution, see LocalFlinkMiniCluster.
     * @param configuration The configuration to load defaults from
     */
    public static void initDefaultsFromConfiguration(Configuration configuration) {

        DEFAULT_WRITE_MODE = FileSystem.WriteMode.OVERWRITE ;


        DEFAULT_OUTPUT_DIRECTORY_MODE =  FileOutputFormat.OutputDirectoryMode.PARONLY;
    }

    // --------------------------------------------------------------------------------------------

    /**
     * The LOG for logging messages in this class.
     */
    private static final Logger LOG = LoggerFactory.getLogger(FileOutputFormat.class);

    /**
     * The key under which the name of the target path is stored in the configuration.
     */
    public static final String FILE_PARAMETER_KEY = "flink.output.file";

    /**
     * The path of the file to be written.
     */
    protected Path outputFilePath;

    /**
     * The write mode of the output.
     */
    private FileSystem.WriteMode writeMode;

    /**
     * The output directory mode
     */
    private FileOutputFormat.OutputDirectoryMode outputDirectoryMode;

    SequenceFile.Writer writer;
    // --------------------------------------------------------------------------------------------

    /** The stream to which the data is written; */
    protected transient FSDataOutputStream stream;

    /** The path that is actually written to (may a a file in a the directory defined by {@code outputFilePath} ) */
    private transient Path actualFilePath;

    /** Flag indicating whether this format actually created a file, which should be removed on cleanup. */
    private transient boolean fileCreated;

    /** The config parameter which defines the fixed length of a record. */
    public static final String BLOCK_SIZE_PARAMETER_KEY = "output.block_size";

    public static final long NATIVE_BLOCK_SIZE = Long.MIN_VALUE;

    /** The block size to use. */
    private long blockSize = NATIVE_BLOCK_SIZE;

    private transient RheemFileOutputFormat.BlockBasedOutput blockBasedOutput;

    private transient DataOutputViewStreamWrapper outView;

    // --------------------------------------------------------------------------------------------

    public RheemFileOutputFormat() {}

    public RheemFileOutputFormat(String path){
        this( new Path(URI.create(path)) );
    }

    public RheemFileOutputFormat(Path outputPath) {
        this.outputFilePath = outputPath;
    }

    public void setOutputFilePath(Path path) {
        if (path == null) {
            throw new IllegalArgumentException("Output file path may not be null.");
        }

        this.outputFilePath = path;
    }

    public Path getOutputFilePath() {
        return this.outputFilePath;
    }


    public void setWriteMode(FileSystem.WriteMode mode) {
        if (mode == null) {
            throw new NullPointerException();
        }
        this.writeMode = mode;
    }

    public FileSystem.WriteMode getWriteMode() {
        return this.writeMode;
    }


    public void setOutputDirectoryMode(FileOutputFormat.OutputDirectoryMode mode) {
        if (mode == null) {
            throw new NullPointerException();
        }

        this.outputDirectoryMode = mode;
    }

    public FileOutputFormat.OutputDirectoryMode getOutputDirectoryMode() {
        return this.outputDirectoryMode;
    }


    // ----------------------------------------------------------------

    @Override
    public void configure(Configuration parameters) {
        try {
            // get the output file path, if it was not yet set
            if (this.outputFilePath == null) {
                // get the file parameter
                String filePath = parameters.getString(FILE_PARAMETER_KEY, null);
                if (filePath == null) {
                    throw new IllegalArgumentException("The output path has been specified neither via constructor/setters" +
                            ", nor via the Configuration.");
                }

                try {
                    this.outputFilePath = new Path(filePath);
                } catch (RuntimeException rex) {
                    throw new RuntimeException("Could not create a valid URI from the given file path name: " + rex.getMessage());
                }
            }

            // check if have not been set and use the defaults in that case
            if (this.writeMode == null) {
                this.writeMode = DEFAULT_WRITE_MODE;
            }

            if (this.outputDirectoryMode == null) {
                this.outputDirectoryMode = DEFAULT_OUTPUT_DIRECTORY_MODE;
            }


            // read own parameters
            this.blockSize = parameters.getLong(BLOCK_SIZE_PARAMETER_KEY, NATIVE_BLOCK_SIZE);
            if (this.blockSize < 1 && this.blockSize != NATIVE_BLOCK_SIZE) {
                throw new IllegalArgumentException("The block size parameter must be set and larger than 0.");
            }
            if (this.blockSize > Integer.MAX_VALUE) {
                throw new UnsupportedOperationException("Currently only block size up to Integer.MAX_VALUE are supported");
            }
        } catch (Exception e){
            throw new RheemException(e);
        }
    }


    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        try {
            if (taskNumber < 0 || numTasks < 1) {
                throw new IllegalArgumentException("TaskNumber: " + taskNumber + ", numTasks: " + numTasks);
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Opening stream for output (" + (taskNumber + 1) + "/" + numTasks + "). WriteMode=" + writeMode +
                        ", OutputDirectoryMode=" + outputDirectoryMode);
            }

            Path p = this.outputFilePath;
            if (p == null) {
                throw new IOException("The file path is null.");
            }

            final FileSystem fs = p.getFileSystem();

            if(fs.exists(p)) {
                fs.delete(p, true);
            }

            this.fileCreated = true;


            final SequenceFile.Writer.Option fileOption = SequenceFile.Writer.file(new org.apache.hadoop.fs.Path(p.toString()));
            final SequenceFile.Writer.Option keyClassOption = SequenceFile.Writer.keyClass(NullWritable.class);
            final SequenceFile.Writer.Option valueClassOption = SequenceFile.Writer.valueClass(BytesWritable.class);
            writer = SequenceFile.createWriter(new org.apache.hadoop.conf.Configuration(true), fileOption, keyClassOption, valueClassOption);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void writeRecord(IT record) throws IOException {
        //this.blockBasedOutput.startRecord();
        try{
            ByteArrayOutputStream b = new ByteArrayOutputStream();
            ObjectOutputStream objStream = new ObjectOutputStream(b);
            objStream.writeObject(record);
            BytesWritable bytesWritable = new BytesWritable(b.toByteArray());
            writer.append(NullWritable.get(), bytesWritable);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    protected String getDirectoryFileName(int taskNumber) {
        return Integer.toString(taskNumber + 1);
    }

    @Override
    public void close() throws IOException {
        try {
            this.writer.close();
            DataOutputViewStreamWrapper o = this.outView;
            if (o != null) {
                o.close();
            }
        }
        finally {
            final FSDataOutputStream s = this.stream;
            if (s != null) {
                this.stream = null;
                s.close();
            }
        }
    }

    /**
     * Initialization of the distributed file system if it is used.
     *
     * @param parallelism The task parallelism.
     */
    @Override
    public void initializeGlobal(int parallelism) throws IOException {
        try {
            final Path path = getOutputFilePath();
            final FileSystem fs = path.getFileSystem();

            // only distributed file systems can be initialized at start-up time.
            if (fs.isDistributedFS()) {

                final FileSystem.WriteMode writeMode = getWriteMode();
                final FileOutputFormat.OutputDirectoryMode outDirMode = getOutputDirectoryMode();

                if (parallelism == 1 && outDirMode == FileOutputFormat.OutputDirectoryMode.PARONLY) {
                    // output is not written in parallel and should be written to a single file.
                    // prepare distributed output path
                    if (!fs.initOutPathDistFS(path, writeMode, false)) {
                        // output preparation failed! Cancel task.
                        throw new IOException("Output path could not be initialized.");
                    }

                } else {
                    // output should be written to a directory

                    // only distributed file systems can be initialized at start-up time.
                    if (!fs.initOutPathDistFS(path, writeMode, true)) {
                        throw new IOException("Output directory could not be created.");
                    }
                }
            }
        }catch (Exception e){
            throw new RheemException(e);
        }
    }

    @Override
    public void tryCleanupOnError() {
        if (this.fileCreated) {
            this.fileCreated = false;

            try {
                close();
            } catch (IOException e) {
                LOG.error("Could not properly close FileOutputFormat.", e);
            }

            try {
                FileSystem.get(this.actualFilePath.toUri()).delete(actualFilePath, false);
            } catch (FileNotFoundException e) {
                // ignore, may not be visible yet or may be already removed
            } catch (Throwable t) {
                LOG.error("Could not remove the incomplete file " + actualFilePath + '.', t);
            }
        }
    }

    /**
     * Writes a block info at the end of the blocks.<br>
     * Current implementation uses only int and not long.
     *
     */
    protected class BlockBasedOutput extends FilterOutputStream {

        private static final int NO_RECORD = -1;

        private final int maxPayloadSize;

        private int blockPos;

        private int blockCount, totalCount;

        private long firstRecordStartPos = NO_RECORD;

        private BlockInfo blockInfo = RheemFileOutputFormat.this.createBlockInfo();

        private DataOutputView headerStream;

        public BlockBasedOutput(OutputStream out, int blockSize) {
            super(out);
            this.headerStream = new DataOutputViewStreamWrapper(out);
            this.maxPayloadSize = blockSize - this.blockInfo.getInfoSize();
        }

        @Override
        public void close() throws IOException {
            if (this.blockPos > 0) {
                this.writeInfo();
            }
            super.flush();
            super.close();
        }

        public void startRecord() {
            if (this.firstRecordStartPos == NO_RECORD) {
                this.firstRecordStartPos = this.blockPos;
            }
            this.blockCount++;
            this.totalCount++;
        }

        @Override
        public void write(byte[] b) throws IOException {
            this.write(b, 0, b.length);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {

            for (int remainingLength = len, offset = off; remainingLength > 0;) {
                int blockLen = Math.min(remainingLength, this.maxPayloadSize - this.blockPos);
                this.out.write(b, offset, blockLen);

                this.blockPos += blockLen;
                if (this.blockPos >= this.maxPayloadSize) {
                    this.writeInfo();
                }
                remainingLength -= blockLen;
                offset += blockLen;
            }
        }

        @Override
        public void write(int b) throws IOException {
            super.write(b);
            if (++this.blockPos >= this.maxPayloadSize) {
                this.writeInfo();
            }
        }

        private void writeInfo() throws IOException {
            this.blockInfo.setRecordCount(this.blockCount);
            this.blockInfo.setAccumulatedRecordCount(this.totalCount);
            this.blockInfo.setFirstRecordStart(this.firstRecordStartPos == NO_RECORD ? 0 : this.firstRecordStartPos);
            RheemFileOutputFormat.this.complementBlockInfo(this.blockInfo);
            this.blockInfo.write(this.headerStream);
            this.blockPos = 0;
            this.blockCount = 0;
            this.firstRecordStartPos = NO_RECORD;
        }
    }
    protected BlockInfo createBlockInfo() {
        return new BlockInfo();
    }
    protected void complementBlockInfo(BlockInfo blockInfo) {}
}
