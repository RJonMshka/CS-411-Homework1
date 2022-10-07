package tasks

import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapred.{MapReduceBase, Mapper, OutputCollector, Reducer, Reporter}

import java.io.IOException
import java.util
import java.util.regex.Pattern

object MRSecondTask {

  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable]:

    @throws[IOException]
    @throws[InterruptedException]
    override def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit = ???

  class Combine extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable]:

    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit = ???

  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable]:

    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit = ???


}
