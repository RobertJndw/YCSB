# pmemkv

This section describes how to run YCSB on pmemkv

### Requirements
Please install [pmemkv-java](https://github.com/pmem/pmemkv-java) and run `mvn install` in the `pmemkv-java` directory. For further instructions and requirements take a look at the [pmemkv-java GitHub repository](https://github.com/pmem/pmemkv-java).

### Build the pmemkv binding
After any modification of the pmemkv-binding you can rebuild the package with this command `mvn -pl site.ycsb:pmemkv-binding -am clean package`. For the complete installtion follow [the official YCSB documentation](https://github.com/brianfrankcooper/YCSB/wiki/Getting-Started).

### Run Interactive Mode
The interactive mode of the YCSB allows to debug and execute single actions. To activate this mode run `./bin/ycsb shell pmemkv` in the main directory of the YCSB. The following list explains the possible commands and their usage.
```
Commands:
  read key [field1 field2 ...] - Read a record
  scan key recordcount [field1 field2 ...] - Scan starting at key
  insert key name1=value1 [name2=value2 ...] - Insert a new record
  update key name1=value1 [name2=value2 ...] - Update a record
  delete key - Delete a record
  table [tablename] - Get or [set] the name of the table
  quit - Quit  
```

### Run Workloads
The executing of a workload consists of two parts, the loading of data and the execution of the workload.
To load the data into the pmemkv run `./bin/ycsb load pmemkv -P workloads/workloada` (the `-P` flag specifies the property file in this case workloada).

After sucessfully loading the data into the database, you can execute the workload with the command `./bin/ycsb run pmemkv -P workloads/workloada`. 

For further information on the possible flags and their meaning, take a look at the [official YCSB documentation](https://github.com/brianfrankcooper/YCSB/wiki/Running-a-Workload).


### pmemkv Configuration Parameters
- `path`: the path for the DB (default: `/mnt/pmem/ycsbDatabase`)
- `engine`: the engine which should be used by the pmemkv (default: `cmap`) (see [list of engines](https://github.com/pmem/pmemkv#storage-engines))
- `size`: the size of the database (default `Integer.MAX_VALUE`)
