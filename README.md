# Native HDFS Fuse Implementation

## Testing

Tested using [fsx](http://svnweb.freebsd.org/base/head/tools/regression/fsx) on a [Hadoop Minicluster](https://hadoop.apache.org/docs/r2.3.0/hadoop-project-dist/hadoop-common/CLIMiniCluster.html). Useful settings include setting the minicluster block size to a small value (e.g. passing <tt>-D dfs.block.size=4194304</tt> as an argument to minicluster) and getting fsx to use a large file (e.g. by passing <tt> -F 134217728</tt>) to test the multi-block logic.
