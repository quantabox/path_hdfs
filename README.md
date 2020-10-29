Python Utilities to work with HDFS and local filesystem
---

Allows you to interact with either local or HDFS files. This enables some of the features similar to pathlib.Path

##### Move experiment between hdfs and local filesystem.
All pyarrow environment variables need to be configured including HADOOP_HOME, ARROW_LIBHDFS_DIR, CLASSPATH with hdfs binary

```python 
from io.path import Path
hdfs_path = Path(<path to hdfs file>)
hdfs_path.copy_file(<path to local filesystem>)
```

License
----
MIT

**Free Software, Hell Yeah!**
