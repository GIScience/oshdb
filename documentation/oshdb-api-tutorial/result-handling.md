# Result Handling

You probably wish to output the result of your computation somewhere.
For a quick and dirty test on your local machine you can just print
the result. However, you should NOT do this when deploying your code
on the cluster.

```
// -- RESULT --
// Don't do it this way on the cluster:
System.out.println(result);
```

## Writing Output the Clean Way

When deploying your code on the cluster, you do not have direct access
to the system. Therefore, a different way of writing output is required.
To this end, the OSHDB-API provides the class
[OutputWriter](https://github.com/GIScience/oshdb/blob/master/oshdb-util/src/main/java/org/heigit/bigspatialdata/oshdb/util/export/OutputWriter.java)
that is able to store the computation output.
There are several options, where the output may be stored:

* write to a File
* write to an H2 database
* write to a PostgreSQL database
* post your result on [Kafka](http://kafka.apache.org/)

Standard methods for Streams of KeyString - ValueString are implemented but you are
free to store any other type.