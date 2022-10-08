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

  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable]:
    val logPattern: Pattern = Pattern.compile(configObject.getString("LogPattern"))
    val logger: Logger = CreateLogger(classOf[Map])

    @throws[IOException]
    @throws[InterruptedException]
    override def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val matcher = logPattern.matcher(value.toString)

      if matcher.matches() then output.collect(new Text(matcher.group(3)), new IntWritable(1))

  @throws[IOException]
  @throws[InterruptedException]
  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable]:
    val logger: Logger = CreateLogger(classOf[Reduce])

    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val sum = values.asScala.reduce((valueOne, valueTwo) => IntWritable(valueOne.get() + valueTwo.get()))

      output.collect(key, IntWritable(sum.get()))

}
