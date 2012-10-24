/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package goraci;

import goraci.generated.CINode;
import goraci.generated.Flushed;
import goraci.generated.cidynamonode;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.gora.dynamodb.query.DynamoDBKey;
import org.apache.gora.dynamodb.store.DynamoDBStore;
import org.apache.gora.store.DataStore;
import org.apache.gora.store.ws.impl.WSDataStoreFactory;
import org.apache.gora.util.GoraException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.amazonaws.auth.BasicAWSCredentials;

/**
 * A Map only job that generates random linked list and stores them using Gora.
 */
public class Generator extends Configured implements Tool {
  
  private static final Log LOG = LogFactory.getLog(Generator.class);
  
  static final int WIDTH = 10;
  static final int WRAP = WIDTH * 2;

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
      public void readFields(DataInput arg0) throws IOException {
        // TODO Auto-generated method stub
        
      }
      
      @Override
      public void write(DataOutput arg0) throws IOException {
        // TODO Auto-generated method stub
        
      }
   }
    
    static class GeneratorRecordReader extends RecordReader<LongWritable,NullWritable> {
      
      private long numNodes;
      private boolean hasNext = true;
      
      @Override
      public void close() throws IOException {
        
      }
      
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

  /**
   * Some ASCII art time:
   * [ . . . ] represents one batch of random longs of length WIDTH
   *
   *                _________________________
   *               |                  ______ |
   *               |                 |      ||
   *             __+_________________+_____ ||
   *             v v                 v     |||
   * first   = [ . . . . . . . . . . . ]   |||
   *             ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^     |||
   *             | | | | | | | | | | |     |||
   * prev    = [ . . . . . . . . . . . ]   |||
   *             ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^     |||
   *             | | | | | | | | | | |     |||
   * current = [ . . . . . . . . . . . ]   |||
   *                                       |||
   * ...                                   |||
   *                                       |||
   * last    = [ . . . . . . . . . . . ]   |||
   *             | | | | | | | | | | |-----|||
   *             |                 |--------||
   *             |___________________________|
   */

  static class GeneratorMapper extends Mapper<LongWritable,NullWritable,NullWritable,NullWritable> {
    
    private boolean concurrent;
    private Object auth;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);
      concurrent = context.getConfiguration().getBoolean("goraci.generator.concurrent", false);
      
      auth = new BasicAWSCredentials( context.getConfiguration().get("goraci.amazon.accessKey", ""),
                                             context.getConfiguration().get("goraci.amazon.secretKey", ""));
    }
    
    @SuppressWarnings("unchecked")
    @Override
    protected void map(LongWritable key, NullWritable value, Context output) {
      DataStore<DynamoDBKey<String, String>, cidynamonode> store = null;
      try {
        long num = key.get();
        System.out.println("num " + num);
        String id = UUID.randomUUID().toString();
        
        store = WSDataStoreFactory.createDataStore(DynamoDBStore.class, DynamoDBKey.class, cidynamonode.class, auth);
        
        DataStore<String,Flushed> flushedTable = null;
	      
	      if (concurrent) {
	        flushedTable = WSDataStoreFactory.getDataStore(String.class, Flushed.class, auth);
	        //flushedTable.createSchema();
	      }
	      
	      store.createSchema();
	      
	      Random rand = new Random();
	      
	      long[] first = null;
	      long[] prev = null;
	      long[] current = new long[WIDTH];
	      
	      long count = 0;
	      while (count < num) {
	        for (int i = 0; i < current.length; i++)
	          current[i] = Math.abs(rand.nextLong());
	        
	        persist(output, store, count, prev, current, id);
	        
	        if (first == null)
	          first = current;
	        prev = current;
	        current = new long[WIDTH];
	        
	        count += current.length;
	        output.setStatus("Count " + count);
	        
	        if (count % WRAP == 0) {
	          // this block of code turns the 1 million linked list of length 25 into one giant circular linked list of 25 million
	          
	          circularLeftShift(first);
	          
	          updatePrev(store, first, prev);
	          
	          if (concurrent) {
	          // keep track of whats flushed in another table, verify can use this info to run concurrently
	       //     Flushed flushed = flushedTable.newPersistent();
	        //    flushed.setCount(count);
	         //   flushedTable.put(id, flushed);
	         //   flushedTable.flush();
	          }

	          first = null;
	          prev = null;
	        }
	      }
	      //if (concurrent)
	        //flushedTable.close();
	} catch (GoraException e) {
      e.printStackTrace();
    } catch (Exception e) {
  		// TODO Auto-generated catch block
  		e.printStackTrace();
	}
    finally{
        if (store != null)
          try {
            store.close();
          } catch (Exception e) {
            e.printStackTrace();
          }
     } 
  }
    
    private static void circularLeftShift(long[] first) {
      long ez = first[0];
      for (int i = 0; i < first.length - 1; i++)
        first[i] = first[i + 1];
      first[first.length - 1] = ez;
    }
    
    private static void persist(Context output, DataStore<DynamoDBKey<String, String>,cidynamonode> store, long count, long[] prev, long[] current, String id) throws Exception {
      System.out.println("Persisting");
      for (int i = 0; i < current.length; i++) {
    	cidynamonode node = new cidynamonode(); //store.newPersistent();
    	node.setState(0);
    	
        node.setCount(count + i);
        if (prev != null)
          node.setPrev(prev[i]);
        else
          node.setPrev(-1);
        
        node.setClient(id);
        
        // Creating object's key
        DynamoDBKey<String, String> newKey = new DynamoDBKey<String, String>();
        newKey.setHashKey(String.valueOf(current[i]));
        node.setHashKey(newKey.getHashKey());
        
        store.put(newKey, node);
        System.out.println("NodeKey:" + current[i] + " NodeValue:"+node.toString());
        
        if (i % 1000 == 0) {
          // Tickle progress every so often else maprunner will think us hung
          output.progress();
        }
      }
      //store.flush();
    }
    
    private static void updatePrev(DataStore<DynamoDBKey<String, String>,cidynamonode> store, long[] first, long[] current) throws IOException {
      System.out.println("Trying to update previous");
      try {
        for (int i = 0; i < current.length; i++) {
        
          cidynamonode node = new cidynamonode();//store.newPersistent();
          node.setPrev(current[i]);
          // Creating object's key
          DynamoDBKey<String, String> newKey = new DynamoDBKey<String, String>();
          newKey.setHashKey(String.valueOf(first[i]));
          node.setHashKey(String.valueOf(first[i]));
          
          store.put(newKey, node);
          System.out.println("[" + i + "] ->" + "- NodeKey:" + first[i] + "Node" + node.toString());
        }
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      //store.flush();
    }
  }
  
  
  @Override
  public int run(String[] args) throws Exception {
    Options options = new Options();
    options.addOption("c", "concurrent", false, "update secondary table with information that allows verification to run concurrently");
    
    GnuParser parser = new GnuParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
      if (cmd.getArgs().length != 4) {
        throw new ParseException("Did not see expected # of arguments, saw " + cmd.getArgs().length);
      }
    } catch (ParseException e) {
      System.err.println("Failed to parse command line " + e.getMessage());
      System.err.println();
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(getClass().getSimpleName() + " <num mappers> <num nodes per map> <accessKey> <secretKey>", options);
      System.exit(-1);
    }

    int numMappers = Integer.parseInt(cmd.getArgs()[0]);
    long numNodes = Long.parseLong(cmd.getArgs()[1]);
    String accessKey = cmd.getArgs()[2];
    String secretKey = cmd.getArgs()[3];
    
    // key = numMappers, value = numNodes
    return run(numMappers, numNodes, accessKey, secretKey, cmd.hasOption("c"));
  }

  public int run(int numMappers, long numNodes, String accessKey, String secretKey, boolean concurrent) throws Exception {
    LOG.info("Running Generator with numMappers=" + numMappers +", numNodes=" + numNodes);
    
    Job job = new Job(getConf());
    
    job.setJobName("Link Generator");
    job.setNumReduceTasks(0);
    job.setJarByClass(getClass());
    
    job.setInputFormatClass(GeneratorInputFormat.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(NullWritable.class);
    
    job.getConfiguration().setInt("goraci.generator.mappers", numMappers);
    job.getConfiguration().setLong("goraci.generator.nodes", numNodes);
    job.getConfiguration().setBoolean("goraci.generator.concurrent", concurrent);
    job.getConfiguration().set("goraci.amazon.accessKey", accessKey);
    job.getConfiguration().set("goraci.amazon.secretKey", secretKey);
    
    job.setMapperClass(GeneratorMapper.class);
    
    job.setOutputFormatClass(NullOutputFormat.class);

    job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);

    boolean success = job.waitForCompletion(true);
    
    return success ? 0 : 1;
  }
  
  public static void main(String[] args) throws Exception {
    int ret = ToolRunner.run(new Generator(), args);
    System.exit(ret);
  }
}
