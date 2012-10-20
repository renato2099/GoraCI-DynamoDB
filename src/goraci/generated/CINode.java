package goraci.generated;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.HashMap;
import org.apache.gora.persistency.StateManager;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.persistency.Persistent;
import org.apache.gora.persistency.impl.StateManagerImpl;
import org.apache.gora.persistency.StatefulHashMap;
import org.apache.gora.persistency.ListGenericArray;

public class CINode implements Persistent {
	
	//public static final Schema _SCHEMA = Schema.parse("{\"type\":\"record\",\"name\":\"CINode\",\"namespace\":\"org.apache.gora.continuous.generated\",\"fields\":[{\"name\":\"prev\",\"type\":\"long\",\"default\":\"-1\"},{\"name\":\"client\",\"type\":\"string\"},{\"name\":\"count\",\"type\":\"long\"}]}");
	  public static enum Field {
	    PREV(0,"prev"),
	    CLIENT(1,"client"),
	    COUNT(2,"count"),
	    ;
	    private int index;
	    private String name;
	    Field(int index, String name) {this.index=index;this.name=name;}
	    public int getIndex() {return index;}
	    public String getName() {return name;}
	    public String toString() {return name;}
	  };
	  
	  public static final String[] _ALL_FIELDS = {"prev","client","count",};
	  
	  //static {
	  //  PersistentBase.registerFields(CINode.class, _ALL_FIELDS);
	  //}
	  
	  private long prev;
	  private String client;
	  private long count;
	  public CINode() {
	    this(new StateManagerImpl());
	  }
	  public CINode(StateManager stateManager) {
	    //super(stateManager);
	  }
	  
	  public CINode newInstance(/*StateManager stateManager*/) {
	    return new CINode();
	  }
	  
	  //public Schema getSchema() { return _SCHEMA; }
	  
	  public Object get(int _field) {
	    switch (_field) {
	    case 0: return prev;
	    case 1: return client;
	    case 2: return count;
	    default: try {
                   throw new Exception("Bad index");
                 } catch (Exception e) {
                   e.printStackTrace();
                   return null;
                 }
	    }
	  }
	  
	  @Override
	  public String toString(){
		  return  "Prev: " + this.prev + " " +
				  "Count: " + this.count + " " +
				  "Client: " + this.client;
	  }
	  
	  @SuppressWarnings(value="unchecked")
	  public void put(int _field, Object _value) {
	    //if(isFieldEqual(_field, _value)) return;
	    //getStateManager().setDirty(this, _field);
	    switch (_field) {
	    case 0:prev = (Long)_value; break;
	    case 1:client = (String)_value; break;
	    case 2:count = (Long)_value; break;
	    default: try{
                   throw new Exception("Bad index");
                 } catch (Exception e) {
                   e.printStackTrace();
                 }
	    }
	  
	  }
	  public long getPrev() {
	    return (Long) get(0);
	  }
	  public void setPrev(long value) {
	    put(0, value);
	  }
	  public String getClient() {
	    return (String) get(1);
	  }
	  public void setClient(String value) {
	    put(1, value);
	  }
	  public long getCount() {
	    return (Long) get(2);
	  }
	  public void setCount(long value) {
	    put(2, value);
	  }
	  
@Override
public CINode clone(){
	return null;
}

@Override
public StateManager getStateManager() {
	// TODO Auto-generated method stub
	return null;
}
@Override
public String[] getFields() {
	// TODO Auto-generated method stub
	return null;
}
@Override
public String getField(int index) {
	// TODO Auto-generated method stub
	return null;
}
@Override
public int getFieldIndex(String field) {
	// TODO Auto-generated method stub
	return 0;
}
@Override
public void clear() {
	// TODO Auto-generated method stub
	
}
@Override
public boolean isNew() {
	// TODO Auto-generated method stub
	return false;
}
@Override
public void setNew() {
	// TODO Auto-generated method stub
	
}
@Override
public void clearNew() {
	// TODO Auto-generated method stub
	
}
@Override
public boolean isDirty() {
	// TODO Auto-generated method stub
	return false;
}
@Override
public boolean isDirty(int fieldIndex) {
	// TODO Auto-generated method stub
	return false;
}
@Override
public boolean isDirty(String field) {
	// TODO Auto-generated method stub
	return false;
}
@Override
public void setDirty() {
	// TODO Auto-generated method stub
	
}
@Override
public void setDirty(int fieldIndex) {
	// TODO Auto-generated method stub
	
}
@Override
public void setDirty(String field) {
	// TODO Auto-generated method stub
	
}
@Override
public void clearDirty(int fieldIndex) {
	// TODO Auto-generated method stub
	
}
@Override
public void clearDirty(String field) {
	// TODO Auto-generated method stub
	
}
@Override
public void clearDirty() {
	// TODO Auto-generated method stub
	
}
@Override
public boolean isReadable(int fieldIndex) {
	// TODO Auto-generated method stub
	return false;
}
@Override
public boolean isReadable(String field) {
	// TODO Auto-generated method stub
	return false;
}
@Override
public void setReadable(int fieldIndex) {
	// TODO Auto-generated method stub
	
}
@Override
public void setReadable(String field) {
	// TODO Auto-generated method stub
	
}
@Override
public void clearReadable(int fieldIndex) {
	// TODO Auto-generated method stub
	
}
@Override
public void clearReadable(String field) {
	// TODO Auto-generated method stub
	
}
@Override
public void clearReadable() {
	// TODO Auto-generated method stub
	
}
@Override
public Persistent newInstance(StateManager stateManager) {
	// TODO Auto-generated method stub
	return null;
}
}
