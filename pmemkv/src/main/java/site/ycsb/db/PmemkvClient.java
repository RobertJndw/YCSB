package site.ycsb.db;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

import java.io.*;
import java.util.*;
import io.pmem.pmemkv.Database;
import io.pmem.pmemkv.Converter;

import java.nio.ByteBuffer;

/**
 * The YCSB binding for <a href="https://github.com/pmem/pmemkv">pmemkv</a>.
 */
public class PmemkvClient extends DB {

  private Database<String, Map<String, ByteIterator>> db;

  @Override
  public void init() throws DBException {
    Properties props = getProperties();

    String path = props.getProperty("path", "/mnt/pmem/ycsbDatabase");
    String engine = props.getProperty("engine", "cmap");
    long size = Long.parseLong(props.getProperty("size", ""+Integer.MAX_VALUE));
    boolean reset = Boolean.parseBoolean(props.getProperty("reset", "false"));

    try {
      File f = new File(path);
      if (f.exists() && !f.isDirectory() && reset) {
        f.delete();
      }
      db = getDatabase(path, size, engine);
    } catch (Exception e) {
      System.err.println("Error: Creating the database failed");
      e.printStackTrace();
      throw new DBException(e);
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    // Return all of them if null or empty
    key = createKey(table, key);
    Map<String, ByteIterator> output = db.getCopy(key);
    // Return null if nothing was found
    if(output == null) {
      return Status.NOT_FOUND;
    }
    if(fields == null || fields.size() == 0) {
      result.putAll(output);
    } else {
      // Only get relevant fields of the output
      for(String f : fields) {
        result.put(f, output.get(f));
      }
    }

    return Status.OK;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    key = createKey(table, key);
    Map<String, ByteIterator> output = db.getCopy(key);
    if(output == null) {
      return Status.NOT_FOUND;
    }
    output.putAll(values);
    db.put(key, output);
    return Status.OK;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {
      key = createKey(table, key);
      db.put(key, values);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return Status.OK;
  }
  
  @Override
  public Status delete(String table, String key) {
    key = createKey(table, key);
    if(db.remove(key)) {
      return Status.OK;
    } else {
      return Status.NOT_FOUND;
    }
  }

  @Override
  public void cleanup() throws DBException {
    db.stop();
  }

  private String createKey(String table, String key) {
    return table+"/"+key;
  }

  private static Database<String, Map<String, ByteIterator>> getDatabase(String path, long size, 
          String engine) {

    Database.Builder<String, Map<String, ByteIterator>> builder = new Database.Builder(engine)
            .setKeyConverter(new StringConverter())
            .setValueConverter(new MapStringConverter());

    File f = new File(path);
    if (f.exists() && !f.isDirectory()) {
      return builder.setPath(path).build();
    } else {
      return builder.setSize(size).setPath(path).setForceCreate(true).build();
    }
  }
}

class StringConverter implements Converter<String> {
  public ByteBuffer toByteBuffer(String entry) {
    return ByteBuffer.wrap(entry.getBytes());
  }

  public String fromByteBuffer(ByteBuffer entry) {
    return new String(entry.array());
  }
}

class MapStringConverter implements Converter<Map<String, ByteIterator>> {
  // Based of this post https://stackoverflow.com/questions/2836646/java-serializable-object-to-byte-array

  @Override
  public ByteBuffer toByteBuffer(Map<String, ByteIterator> stringByteIteratorMap) {
    ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
    ObjectOutputStream out;
    try {
      out = new ObjectOutputStream(byteOut);
      // Conversion necessary because ByteIterator is not serializable
      out.writeObject(StringByteIterator.getStringMap(stringByteIteratorMap));
      out.flush();
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      // Make sure the stream is closed at the end
      try {
        byteOut.close();
      } catch (IOException ignored) {
        ignored.printStackTrace();
      }
    }
    return ByteBuffer.wrap(byteOut.toByteArray());
  }

  @Override
  public Map<String, ByteIterator> fromByteBuffer(ByteBuffer byteBuffer) {
    ByteArrayInputStream byteIn = new ByteArrayInputStream(byteBuffer.array());
    ObjectInputStream in = null;
    Map<String, String> map = new HashMap<>();
    try {
      in = new ObjectInputStream(byteIn);
      map = (Map<String, String>) in.readObject();
    } catch (IOException | ClassNotFoundException e) {
      e.printStackTrace();
    } finally {
      try {
        // Make sure the input stream is closed
        if (in != null) {
          in.close();
        }
      } catch (IOException ignored) {
        ignored.printStackTrace();
      }
    }
    // Revert conversion from serialization
    return StringByteIterator.getByteIteratorMap(map);
  }
}

