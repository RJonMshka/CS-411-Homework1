package tasks

import scala.jdk.CollectionConverters.*
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.io.{IntWritable, LongWritable, NullWritable, Text, WritableComparable, WritableComparator}
import org.apache.hadoop.mapred.{MapReduceBase, Mapper, OutputCollector, Reducer, Reporter}
import org.slf4j.Logger
import tasks.HelperUtils.{CreateLogger, TimeUtil}

import java.io.IOException
import java.util
import java.util.regex.Pattern


/**
 * This object is a wrapper for Mapper and Reducer of second task
 */
object MRSecondTask {
  val configObject: Config = ConfigFactory.load().getConfig("mapReduceTasksConfig")

  class IntermediateMap extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable]:
    val logger: Logger = CreateLogger(classOf[IntermediateMap])

    val logPattern: Pattern = Pattern.compile(configObject.getString("LogPattern"))
    val stringMessagePattern: Pattern = Pattern.compile(configObject.getString("StringMessagePattern"))

    @throws[IOException]
    @throws[InterruptedException]
    override def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      logger.info("map function called")
      val intervalInSeconds: Int = configObject.getInt("MRSecondTaskIntervalInSeconds")

      val matcher = logPattern.matcher(value.toString)
      if(matcher.matches()) {
        logger.info("log message matched with the log pattern")
        val stringInstanceMatcher = stringMessagePattern.matcher(matcher.group(5))

        if matcher.group(3) == configObject.getString("MessageTypeError") && stringInstanceMatcher.matches() then
          logger.info("error message matches the string pattern is: {}", matcher.group(5))
          val (timeStartString, timeEndString) = TimeUtil.convertToHourMinuteInterval(matcher.group(1), intervalInSeconds)
          output.collect(new Text(timeStartString.concat("-").concat(timeEndString)), new IntWritable(1))

      }

  @throws[IOException]
  @throws[InterruptedException]
  class IntermediateReduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable]:
    val logger: Logger = CreateLogger(classOf[IntermediateReduce])

    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      logger.info("reducer function called")
      val sum = values.asScala.reduce((valueOne, valueTwo) => IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key, new IntWritable(sum.get()))

  class SortComparator() extends WritableComparator(classOf[Text], true):
    override def compare(a: WritableComparable[_], b: WritableComparable[_]): Int =
      val textAArray = a.asInstanceOf[Text].toString.split(configObject.getString("OutputFormatSeparator"))
      val textBArray = b.asInstanceOf[Text].toString.split(configObject.getString("OutputFormatSeparator"))
      // compares the count part of "timeInterval,count" formatted key
      val comparison = textAArray(1).toInt.compareTo(textBArray(1).toInt)
      // if the count clashes, compare the timeInterval part
      if comparison == 0 then
        -1 * textAArray(0).compareTo(textBArray(0))
      else
        -1 * comparison

  @throws[IOException]
  @throws[InterruptedException]
  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, NullWritable]:
    val logger = CreateLogger(classOf[Map])
    val inputDataPattern: Pattern = Pattern.compile(configObject.getString("IntermediateTaskOutputPattern"))
    val nullVal: NullWritable = NullWritable.get()

    override def map(key: LongWritable, value: Text, output: OutputCollector[Text, NullWritable], reporter: Reporter): Unit =
      logger.info("map function called")
      val matcher = inputDataPattern.matcher(value.toString)
      // if the key matches "timeInterval,count" format, only then proceed to output the result
      if matcher.matches() then
        output.collect(value, nullVal)

  @throws[IOException]
  @throws[InterruptedException]
  class Reduce extends MapReduceBase with Reducer[Text, NullWritable, Text, IntWritable]:
    val logger = CreateLogger(classOf[Reduce])
    val inputDataPattern: Pattern = Pattern.compile(configObject.getString("IntermediateTaskOutputPattern"))

    override def reduce(key: Text, values: util.Iterator[NullWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      logger.info("reducer function called")
      val matcher = inputDataPattern.matcher(key.toString)

      // if the key matches "timeInterval,count" format, only then proceed to output the result
      if matcher.matches() then
        output.collect(new Text(matcher.group(1)), new IntWritable(matcher.group(2).toInt))


}
