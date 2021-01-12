package site.ycsb.db;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

import java.util.*;
import io.pmem.pmemkv.Database;

/**
 * The YCSB binding for <a href="https://github.com/pmem/pmemkv">pmemkv</a>.
 */
public class PmemkvClient extends DB {

  private Database<String, Map<String, ByteIterator>> db;

  @Override
  public void init() throws DBException {
    Properties props = getProperties();

    String path = props.getProperty("path", "/dev/shm");
    String engine = props.getProperty("engine", "vsmap");

    try {
      db = new Database.Builder<String, Map<String, ByteIterator>>(engine)
          .setPath(path)
          .build();
    } catch (Exception e) {
      System.err.println("Error: Creating the database failed");
      throw new DBException(e);
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    return null;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    return null;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    return null;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    return null;
  }

  //Delete a single record
  @Override
  public Status delete(String table, String key){
    return null;
  }
}
