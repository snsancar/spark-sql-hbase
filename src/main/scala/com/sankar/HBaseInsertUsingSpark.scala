package com.sankar
import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
object HBaseInsertUsingSpark extends App {

  val sc = new SparkContext("local", "HBase Spark App")
  val fileRDD = sc.textFile(args(0), 2)
  val transformedRDD = fileRDD.map { line => convertToKeyValuePairs(line) }

  val conf = HBaseConfiguration.create()
  conf.set(TableOutputFormat.OUTPUT_TABLE, "payment")
  conf.set("hbase.zookeeper.quorum", "localhost:2181")
  conf.set("hbase.master", "localhost:60000")
  conf.set("fs.default.name", "hdfs://localhost:8020")
  conf.set("hbase.rootdir", "/hbase")

  val jobConf = new Configuration(conf)
  jobConf.set("mapreduce.job.output.key.class", classOf[Text].getName)
  jobConf.set("mapreduce.job.output.value.class", classOf[LongWritable].getName)
  jobConf.set("mapreduce.outputformat.class", classOf[TableOutputFormat[Text]].getName)

  transformedRDD.saveAsNewAPIHadoopDataset(jobConf)

  /**
   * convert the record to key, value pairs.
   */
  def convertToKeyValuePairs(line: String): (ImmutableBytesWritable, Put) = {

    val cfDataBytes = Bytes.toBytes("cf")
    val rowkey = Bytes.toBytes(line.split("\\|")(1))
    val put = new Put(rowkey)

    put.add(cfDataBytes, Bytes.toBytes("PaymentDate"), Bytes.toBytes(line.split("|")(0)))
    put.add(cfDataBytes, Bytes.toBytes("PaymentNumber"), Bytes.toBytes(line.split("|")(1)))
    put.add(cfDataBytes, Bytes.toBytes("VendorName"), Bytes.toBytes(line.split("|")(2)))
    put.add(cfDataBytes, Bytes.toBytes("Category"), Bytes.toBytes(line.split("|")(3)))
    put.add(cfDataBytes, Bytes.toBytes("Amount"), Bytes.toBytes(line.split("|")(4)))
    return (new ImmutableBytesWritable(rowkey), put)
  }
}