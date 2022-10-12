package tasks

import scala.jdk.CollectionConverters.*
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapred.{MapReduceBase, Mapper, OutputCollector, Reducer, Reporter}
import org.slf4j.Logger
import tasks.HelperUtils.CreateLogger
import tasks.MRFirstTask.configObject

import java.io.IOException
import java.util
import java.util.regex.Pattern


/**
 * This object is a wrapper for Mapper and Reducer of third task
 */
object MRThirdTask {
  val configObject: Config = ConfigFactory.load().getConfig("mapReduceTasksConfig")

  /**
   *  Mapper class for third task
   */
  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable]:
    val logPattern: Pattern = Pattern.compile(configObject.getString("LogPattern"))
    val logger: Logger = CreateLogger(classOf[Map])

    /**
     * This method matches the log message to the log pattern, if matches, writes to output with log message type as key and a single integer 1 as value
     * @param key - LongWritable object - we do not use this key anywhere, however this is the key for a single entry to map method
     * @param value - a single log text
     * @param output - output object which needs to written to
     * @param reporter - monitoring object
     * @throws IOException - IOException
     * @throws InterruptedException - InterruptedException
     */
    @throws[IOException]
    @throws[InterruptedException]
    override def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val matcher = logPattern.matcher(value.toString)

      if matcher.matches() then output.collect(new Text(matcher.group(3)), new IntWritable(1))

  /**
   *  Reducer class for third task
   */
  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable]:
    val logger: Logger = CreateLogger(classOf[Reduce])

    /**
     * This method get the input as key value pair, which are the outputs of mapper group together
     * Sums all values of "values" together and outputs the key, sum as pair
     * @param key - Text key which will be the same one that is passed in mapper's output
     * @param values - all the aggregated values that belong to a single key
     * @param output - output object which needs to written to
     * @param reporter - monitoring object
     * @throws IOException - IOException
     * @throws InterruptedException - InterruptedException
     */
    @throws[IOException]
    @throws[InterruptedException]
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val sum = values.asScala.reduce((valueOne, valueTwo) => IntWritable(valueOne.get() + valueTwo.get()))

      output.collect(key, IntWritable(sum.get()))

}
