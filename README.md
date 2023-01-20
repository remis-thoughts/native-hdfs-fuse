# Native HDFS Fuse Implementation

This program allows you to mount an HDFS via [FUSE](http://fuse.sourceforge.net) so it appears like a local filesystem. 

Unlike [other FUSE HDFS implementations](https://wiki.apache.org/hadoop/MountableHDFS) this implementation doesn't use [libhdfs](https://wiki.apache.org/hadoop/LibHDFS) or otherwise start a JVM - it constructs and sends the protocol buffer messages itself. The implementation supports random file writes too.

## Usage

### Compiling

<details closed>
<summary>Dependencies for Ubuntu (22.04)</summary>

```bash
sudo apt-get install -y pkgconf libfuse-dev libprotobuf-c-dev libprotobuf-dev protobuf-c-compiler uncrustify
```

</details>

Compile the program :

```sh
make && make install
```

This will compile and install the <tt>native-hdfs-fuse</tt> binary to <tt>/usr/bin</tt>. The build process needs the [protoc-c](https://github.com/protobuf-c/protobuf-c) protobuf compiler available and uses [pkg-config](http://www.freedesktop.org/wiki/Software/pkg-config) to find the <tt>fuse</tt> and <tt>libprotobuf-c</tt> shared libraries it needs to link to.

You can make a debug build using <tt>make debug</tt>; this adds debug symbols to the binary and compiles in verbose logging statements.

### Running

```sh
native-hdfs-fuse <namenode host> <namenode port, usually 8020> <other FUSE arguments, including mount directory>
```

## Testing

Tested using [fsx](http://svnweb.freebsd.org/base/head/tools/regression/fsx) on a [Hadoop Minicluster](https://hadoop.apache.org/docs/r2.3.0/hadoop-project-dist/hadoop-common/CLIMiniCluster.html). Useful settings include setting the minicluster block size to a small value (e.g. passing <tt>-D dfs.block.size=4194304</tt> as an argument to minicluster) and getting fsx to use a large file (e.g. by passing <tt> -F 134217728</tt>) to test the multi-block logic.

## Contributing

Please contribute by submitting Github pull requests [here](???).

Some missing features:

- Support for HDFS encrypted transport.
- Using HDFS' shared memory shortcut when reading blocks locally.
- CRC32 (not CRC32C) checksumming.
- Checksum validation when reading packets.
- Data Node pipeline recovery.
