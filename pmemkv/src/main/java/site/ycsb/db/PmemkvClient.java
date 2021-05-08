package site.ycsb.db;

import io.pmem.pmemkv.DatabaseException;
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

  private static final String PROPERTY_PATH = "pmemkv.path";
  private static final String PROPERTY_ENGINE = "pmemkv.engine";
  private static final String PROPERTY_SIZE = "pmemkv.size";


  private Database<String, Map<String, ByteIterator>> db;
  private String path, engine;
  private long size;
  private static int threads = 0;


  @Override
  public void init() throws DBException {
    synchronized (PmemkvClient.class) {
      final Properties props = getProperties();
      engine = props.getProperty(PROPERTY_ENGINE, "cmap");
      path = props.getProperty(PROPERTY_PATH);
      if (path == null) {
        throw new DBException(PROPERTY_PATH + " is mandatory to run");
      }
      size = Long.parseLong(props.getProperty(PROPERTY_SIZE));
      if (path == null) {
        throw new DBException(PROPERTY_PATH + " is mandatory to run");
      }

      // Create database here
      Database.Builder<String, Map<String, ByteIterator>> builder = new Database.Builder(engine)
          .setPath(path)
          .setKeyConverter(new StringConverter())
          .setValueConverter(new MapStringConverter());
      try {
        File f = new File(path);
        if (f.exists() && !f.isDirectory()) {
          db = builder.build();
        } else {
          db = builder
              .setSize(size)
              .setForceCreate(true)
              .build();
        }
      } catch (DatabaseException e) {
        throw new DBException(String.format("Error: Creating the database failed: %s %s", engine, path));
      }
      threads++;
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
    // If no field where requested return all fields
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
    if (!engine.equals("vsmap")) {
      return Status.NOT_IMPLEMENTED;
    }
    startkey = createKey(table, startkey);
    db.getAbove(startkey, (k, v) -> {
        // Maybe there is a better option to exit sooner
        if(result.size() == recordcount) {
          return;
        }
        result.add(new HashMap<>(v));
      });
    return Status.OK;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    key = createKey(table, key);
    Map<String, ByteIterator> output = db.getCopy(key);
    if(output == null) {
      return Status.NOT_FOUND;
    }
    output.putAll(values);
    try {
      db.put(key, output);
    } catch (DatabaseException e) {
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {
      key = createKey(table, key);
      db.put(key, values);
    } catch (Exception e) {
      return Status.ERROR;
    }
    return Status.OK;
  }
  
  @Override
  public Status delete(String table, String key) {
    key = createKey(table, key);
    try {
      if(db.remove(key)) {
        return Status.OK;
      } else {
        return Status.NOT_FOUND;
      }
    } catch (DatabaseException e) {
      return Status.ERROR;
    }
  }

  @Override
  public void cleanup() {
    synchronized (PmemkvClient.class) {
      threads--;
      if(db != null && threads == 0) {
        db.stop();
        db = null;
      }
    }
  }

  private String createKey(String table, String key) {
    return table+"/"+key;
  }
}

class StringConverter implements Converter<String> {
  public ByteBuffer toByteBuffer(String entry) {
    return ByteBuffer.wrap(entry.getBytes());
  }

  public String fromByteBuffer(ByteBuffer entry) {
    byte[] bytes = new byte[entry.capacity()];
    entry.get(bytes);
    return new String(bytes);
  }
}

class MapStringConverter implements Converter<Map<String, ByteIterator>> {
  // Based on this post https://stackoverflow.com/questions/2836646/java-serializable-object-to-byte-array

  @Override
  public ByteBuffer toByteBuffer(Map<String, ByteIterator> stringByteIteratorMap) {
    try (final ByteArrayOutputStream byteOut = new ByteArrayOutputStream()) {
      final ObjectOutputStream out = new ObjectOutputStream(byteOut);
      out.writeObject(StringByteIterator.getStringMap(stringByteIteratorMap));
      out.flush();
      return ByteBuffer.wrap(byteOut.toByteArray());
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public Map<String, ByteIterator> fromByteBuffer(ByteBuffer byteBuffer) {
    byte[] bytes = new byte[byteBuffer.capacity()];
    byteBuffer.get(bytes);

    Map<String, String> map = new HashMap<>();
    try (final ByteArrayInputStream byteIn = new ByteArrayInputStream(bytes)) {
      ObjectInputStream in = new ObjectInputStream(byteIn);
      map = (Map<String, String>) in.readObject();
    } catch (IOException | ClassNotFoundException e) {
      e.printStackTrace();
    }
    return StringByteIterator.getByteIteratorMap(map);
  }
}

