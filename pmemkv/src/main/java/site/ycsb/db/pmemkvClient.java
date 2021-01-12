package site.ycsb.db;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

/**
 * The YCSB binding for <a href="https://github.com/pmem/pmemkv">pmemkv</a>.
 */
public class pmemkvClient extends site.ycsb.DB {

  @Override
  public void init() throws DBException {

  }

  //Read a single record
  @Override
  public int read(String table, String key, Set<String> fields, HashMap<String,String> result){
      return 0;
  }

  //Perform a range scan
  @Override
  public int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String,String>> result){
    return 0;
  }

  //Update a single record
  @Override
  public int update(String table, String key, HashMap<String,String> values){
    return 0;
  }

  //Insert a single record
  @Override
  public int insert(String table, String key, HashMap<String,String> values){
    return 0;
  }

  //Delete a single record
  @Override
  public int delete(String table, String key){
    return 0;
  }
}
