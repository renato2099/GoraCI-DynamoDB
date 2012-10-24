package goraci.generated;

import java.util.Set;

import org.apache.gora.persistency.Persistent;
import org.apache.gora.persistency.StateManager;
import com.amazonaws.services.dynamodb.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodb.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodb.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodb.datamodeling.DynamoDBTable;

@DynamoDBTable(tableName = "cidynamonode")
public class cidynamonode implements Persistent {
  
    @Override
    public String toString(){
      return "ID:"+id+"\tCount:"+"\tClient:"+client+"\tPrev:"+String.valueOf(prev);
    }
    
    private String id;

    @DynamoDBHashKey(attributeName="id") 
    public String getHashKey() {  return id; } 
    public void setHashKey(String pId){  this.id = pId; }

    private double count;
    @DynamoDBAttribute(attributeName = "Count")
    public double getCount() {  return count;  }
    public void setCount(double pCount) {  this.count = pCount;  }

    private String client;
    @DynamoDBAttribute(attributeName = "Client")
    public String getClient() {  return client;  }
    public void setClient(String pClient) {  this.client = pClient;  }

    private long prev;
    @DynamoDBAttribute(attributeName = "Prev")
    public long getPrev() {  return prev;  }
    public void setPrev(long pPrev) {  this.prev = pPrev;  }

    private double state;
    @DynamoDBAttribute(attributeName = "State")
    public double getState() {  return state;  }
    public void setState(double pState) {  this.state = pState;  }

    public void setNew(boolean pNew){}
    public void setDirty(boolean pDirty){}
    @Override
    public StateManager getStateManager() { return null; }
    @Override
    public Persistent newInstance(StateManager stateManager) { return null; }
    @Override
    public String[] getFields() { return null; }
    @Override
    public String getField(int index) {	return null; }
    @Override
    public int getFieldIndex(String field) { return 0; }
    @Override
    public void clear() { }
    @Override
    public cidynamonode clone() {	return null; }
    @Override
    public boolean isNew() { return false; }
    @Override
    public void setNew() { }
    @Override
    public void clearNew() {	}
    @Override
    public boolean isDirty() { return false; }
    @Override
    public boolean isDirty(int fieldIndex) { return false; }
    @Override
    public boolean isDirty(String field) { return false; }
    @Override
    public void setDirty() { }
    @Override
    public void setDirty(int fieldIndex) { }
    @Override
    public void setDirty(String field) { }
    @Override
    public void clearDirty(int fieldIndex) { }
    @Override
    public void clearDirty(String field) { }
    @Override
    public void clearDirty() { }
    @Override
    public boolean isReadable(int fieldIndex) {	return false; }
    @Override
    public boolean isReadable(String field) { return false; }
    @Override
    public void setReadable(int fieldIndex) { }
    @Override
    public void setReadable(String field) { }
    @Override
    public void clearReadable(int fieldIndex) { }
    @Override
    public void clearReadable(String field) { }
    @Override
    public void clearReadable() { }
}
