package goraci;

import goraci.Generator.GeneratorInputFormat.GeneratorInputSplit;
import goraci.Generator.GeneratorInputFormat.GeneratorRecordReader;
import goraci.Verify.VerifyMapper;
import goraci.Verify.VerifyReducer;
import goraci.generated.cidynamonode;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.gora.dynamodb.query.DynamoDBKey;
import org.apache.gora.dynamodb.store.DynamoDBStore;
import org.apache.gora.mapreduce.GoraMapper;
import org.apache.gora.query.Query;
import org.apache.gora.store.DataStore;
import org.apache.gora.store.ws.impl.WSDataStoreFactory;
import org.apache.gora.util.GoraException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import com.amazonaws.auth.BasicAWSCredentials;

/**
 * A Map Reduce job that verifies that the linked list generated by {@link goraci.Generator} do not have any holes.
 */
public class VerifyDynamoDB extends Configured implements Tool{

  private static final Log LOG = LogFactory.getLog(Verify.class);

  private Job job;
  
  private Object auth;
  
  @Override
  public int run(String[] args) throws Exception {
    Options options = new Options();
    options.addOption("c", "concurrent", false, "run concurrently with generation");
    
    GnuParser parser = new GnuParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
      if (cmd.getArgs().length != 5) {
        throw new ParseException("Did not see expected # of arguments, saw " + cmd.getArgs().length);
      }
    } catch (ParseException e) {
      System.err.println("Failed to parse command line " + e.getMessage());
      System.err.println();
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(getClass().getSimpleName() + " <output dir> <total nodes>", options);
      System.exit(-1);
    }

    String outputDir = cmd.getArgs()[0];
    int totNodes = Integer.parseInt(cmd.getArgs()[1]);
    int numReducers = Integer.parseInt(cmd.getArgs()[2]);
    String accessKey = cmd.getArgs()[3];
    String secretKey = cmd.getArgs()[4];

    return run(outputDir, numReducers, totNodes, cmd.hasOption("c"), accessKey, secretKey);
  }

  public int run(String outputDir, int numReducers, int totNodes, boolean concurrent, String accessKey, String secretKey) throws Exception {
    return run(new Path(outputDir), numReducers, totNodes, concurrent, accessKey, secretKey);
  }
  
  public int run(Path outputDir, int numReducers, int totNodes, boolean concurrent, String accessKey, String secretKey) throws Exception {
    // Running the job
    start(outputDir, numReducers, totNodes, concurrent, accessKey, secretKey);
    // Waiting for the job to be completed
    boolean success = job.waitForCompletion(true);
    // Whether job's execution was successful  
    return success ? 0 : 1;
  }
  
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public void start(Path outputDir, int numReducers, int totNodes, boolean concurrent, String accessKey, String secretKey) throws GoraException, IOException, Exception {
    LOG.info("Running Verify with outputDir=" + outputDir +", totNodes=" + totNodes);
    
    //DataStore<Long,CINode> store = DataStoreFactory.getDataStore(Long.class, CINode.class, new Configuration());
    auth = new BasicAWSCredentials( accessKey, secretKey);
    
    DataStore<Long,cidynamonode> store = WSDataStoreFactory.createDataStore(DynamoDBStore.class, DynamoDBKey.class, cidynamonode.class, auth);

    job = new Job(getConf());
    
    if (!job.getConfiguration().get("io.serializations").contains("org.apache.hadoop.io.serializer.JavaSerialization")) {
      job.getConfiguration().set("io.serializations", job.getConfiguration().get("io.serializations") + ",org.apache.hadoop.io.serializer.JavaSerialization");
    }

    job.setJobName("Link Verifier");
    job.setNumReduceTasks(numReducers);
    job.setJarByClass(getClass());
    
    Query query = store.newQuery();
    //if (!concurrent) {
      // no concurrency filtering, only need prev field
      //query.setFields("prev");
    //} else {
      //readFlushed(job.getCon  figuration());
    //}

    GoraMapper.initMapperJob(job, query, store, DynamoDBKey.class, VLongWritable.class, VerifyMapper.class, true);

    job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);
    
    job.setReducerClass(VerifyReducer.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(job, outputDir);

    store.close();
    
    job.submit();
  }
  


  
  
  static class GeneratorInputFormat extends InputFormat<LongWritable,NullWritable> {
    
    static class GeneratorInputSplit extends InputSplit implements Writable {
      
      @Override
      public long getLength() throws IOException, InterruptedException {
        return 1;
      }
      
      @Override
      public String[] getLocations() throws IOException, InterruptedException {
        return new String[0];
      }
      
      @Override
      public void readFields(DataInput arg0) throws IOException {      }
      
      @Override
      public void write(DataOutput arg0) throws IOException {        }
   }
    
    static class GeneratorRecordReader extends RecordReader<LongWritable,NullWritable> {
      
      private long numNodes;
      private boolean hasNext = true;
      
      @Override
      public void close() throws IOException {      }
      
      @Override
      public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return new LongWritable(numNodes);
      }
      
      @Override
      public NullWritable getCurrentValue() throws IOException, InterruptedException {
        return NullWritable.get();
      }
      
      @Override
      public float getProgress() throws IOException, InterruptedException {
        return 0;
      }
      
      @Override
      public void initialize(InputSplit arg0, TaskAttemptContext context) throws IOException, InterruptedException {
        numNodes = context.getConfiguration().getLong("goraci.generator.nodes", 1000000);
      }
      
      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException {
        boolean hasnext = this.hasNext;
        this.hasNext = false;
        return hasnext;
      }
      
    }
    
    @Override
    public RecordReader<LongWritable,NullWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
      GeneratorRecordReader rr = new GeneratorRecordReader();
      rr.initialize(split, context);
      return rr;
    }
    
    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException, InterruptedException {
      int numMappers = job.getConfiguration().getInt("goraci.generator.mappers", 1);
      
      ArrayList<InputSplit> splits = new ArrayList<InputSplit>(numMappers);
      
      for (int i = 0; i < numMappers; i++) {
        splits.add(new GeneratorInputSplit());
      }
      
      return splits;
    }
    
  }
  
}
