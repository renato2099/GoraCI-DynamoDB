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

import goraci.generated.cidynamonode;

import java.io.IOException;
import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.gora.dynamodb.query.DynamoDBKey;
import org.apache.gora.dynamodb.query.DynamoDBQuery;
import org.apache.gora.dynamodb.store.DynamoDBStore;
import org.apache.gora.query.Query;
import org.apache.gora.query.Result;
import org.apache.gora.store.DataStore;
import org.apache.gora.store.ws.impl.WSDataStoreFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.amazonaws.auth.BasicAWSCredentials;

/**
 * A stand alone program that follows a linked list created by {@link Generator} and prints timing info.
 */
public class Walker extends Configured implements Tool {
  
  private static final String[] PREV_FIELD = new String[] {"prev"};

  private Object auth;
  
  private void setAuth(String accessKey, String secretKey){
    auth = new BasicAWSCredentials(accessKey, secretKey);
  }
  
  @SuppressWarnings("unchecked")
  public int run(String[] args) throws IOException {
    Options options = new Options();
    options.addOption("n", "num", true, "number of queries");
    options.addOption("ak","accessKey", true, "Amazon Access Key");
    options.addOption("sk","secretKey", true, "Amazon Secret Key");
    
    GnuParser parser = new GnuParser();
    CommandLine cmd = null;
    
    try {
      
      cmd = parser.parse(options, args);
      //if (cmd.getArgs().length != 2) {
      //  throw new ParseException("Incorrect number of arguments");
      //}
      setAuth(cmd.getArgs()[0], cmd.getArgs()[1]);
      
    } catch (ParseException e) {
      System.err.println("Failed to parse command line " + e.getMessage());
      System.err.println();
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(getClass().getSimpleName(), options);
      System.exit(-1);
    }
    
    long maxQueries = Long.MAX_VALUE;
    if (cmd.hasOption('n')) {
      maxQueries = Long.parseLong(cmd.getOptionValue("n"));
    }

    DataStore<DynamoDBKey<String, String>, cidynamonode> store =  WSDataStoreFactory.createDataStore(DynamoDBStore.class, 
                                                                                                     DynamoDBKey.class, 
                                                                                                     cidynamonode.class, 
                                                                                                     auth);
  
    Random rand = new Random();

    long numQueries = 0;
    
    try {
      while (numQueries < maxQueries) {
        cidynamonode node = findStartNode(rand, store);
        numQueries++;
      
        while (node != null && node.getPrev() >= 0 && numQueries < maxQueries) {

          double prev = (double)node.getPrev();

          long t1 = System.currentTimeMillis();
        
          // Defining key to retrieve object
          DynamoDBKey<String, String> prevKey = new DynamoDBKey<String, String>();
          prevKey.setHashKey(String.valueOf(prev));
          // Retrieving object
          node = store.get(prevKey);
          
          long t2 = System.currentTimeMillis();
          System.out.printf("CQ %d %2$,.12f  \n", t2 - t1, prev);
          numQueries++;
        
          t1 = System.currentTimeMillis();
          node = store.get(prevKey, PREV_FIELD);
          t2 = System.currentTimeMillis();
          System.out.printf("HQ %d %2$,.12f \n", t2 - t1, prev);
          numQueries++;
        }
      }
    store.close();
    
    } catch (Exception e) {
      e.printStackTrace();
    }
    
    return 0;
  }
  
  @SuppressWarnings({ "static-access", "rawtypes" })
  private static cidynamonode findStartNode(Random rand, DataStore<DynamoDBKey<String, String>,cidynamonode> store) {
    try {
      Query<DynamoDBKey<String, String>,cidynamonode> query = store.newQuery();
      ((DynamoDBQuery)(query)).setType(DynamoDBQuery.SCAN_QUERY);
      DynamoDBKey<String, String> randKey = new DynamoDBKey<String, String>();
      randKey.setHashKey(String.valueOf(rand.nextLong()));
      
      query.setStartKey(randKey);
      query.setKey(randKey);
      query.setLimit(1);
      query.setFields(PREV_FIELD);
      
      long t1 = System.currentTimeMillis();
      Result<DynamoDBKey<String, String>, cidynamonode> rs;
      
        rs = store.execute(query);
      
      long t2 = System.currentTimeMillis();
      
      if (rs.next()) {
        System.out.printf("FSR %d %016x\n", t2 - t1, rs.getKey());
        return rs.get();
      }
      
      System.out.println("FSR " + (t2 - t1));
    
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
    return null;
  }
  
  public static void main(String[] args) throws Exception {
    int ret = ToolRunner.run(new Walker(), args);
    System.exit(ret);
  }
  
}
