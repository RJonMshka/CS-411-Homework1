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

  /**
   *  Intermediate Mapper class for second task
   */
  class IntermediateMap extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable]:
    val logger: Logger = CreateLogger(classOf[IntermediateMap])

    val logPattern: Pattern = Pattern.compile(configObject.getString("LogPattern"))
    val stringMessagePattern: Pattern = Pattern.compile(configObject.getString("StringMessagePattern"))

    /**
     * This method matches the log message to the log pattern, if matches, writes to output with time interval as key and a single integer 1 as value
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

  /**
   *  Intermediate reducer class for second task
   */
  class IntermediateReduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable]:
    val logger: Logger = CreateLogger(classOf[IntermediateReduce])

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
      logger.info("reducer function called")
      val sum = values.asScala.reduce((valueOne, valueTwo) => IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key, new IntWritable(sum.get()))

  /**
   * Secondary sort key comparator class, used to sort the grouped outputs of map which needs to be passed to Reducer
   */
  class SortComparator() extends WritableComparator(classOf[Text], true):
    /**
     * This method is used to decide sort criteria for keys that needs to be fed to the reducer
     * First the count is compared and if count is same, then time interval is compared
     * Used to sort keys in descending order of count and then descending order of time interval
     * @param a - WritableComparable key
     * @param b - WritableComparable another key for comparison
     * @return - Int - -1 for if a > b, 1 if a < b and 0 if both are exactly same
     */
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

  /**
   *  Mapper class for second task
   */
  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, NullWritable]:
    val logger: Logger = CreateLogger(classOf[Map])
    val inputDataPattern: Pattern = Pattern.compile(configObject.getString("IntermediateTaskOutputPattern"))
    val nullVal: NullWritable = NullWritable.get()


    /**
     * This method matches the intermediate output text "(time interval, count) format" to the a reducer input data pattern, if matches, writes to output with whole text as key and NullWritable object as value
     * @param key - LongWritable object - we do not use this key anywhere, however this is the key for a single entry to map method
     * @param value - a single log text
     * @param output - output object which needs to written to
     * @param reporter - monitoring object
     * @throws IOException - IOException
     * @throws InterruptedException - InterruptedException
     */
    @throws[IOException]
    @throws[InterruptedException]
    override def map(key: LongWritable, value: Text, output: OutputCollector[Text, NullWritable], reporter: Reporter): Unit =
      logger.info("map function called")
      val matcher = inputDataPattern.matcher(value.toString)
      // if the key matches "timeInterval,count" format, only then proceed to output the result
      if matcher.matches() then
        output.collect(value, nullVal)

  /**
   *  Reducer class for second task
   */
  class Reduce extends MapReduceBase with Reducer[Text, NullWritable, Text, IntWritable]:
    val logger: Logger = CreateLogger(classOf[Reduce])
    val inputDataPattern: Pattern = Pattern.compile(configObject.getString("IntermediateTaskOutputPattern"))

    /**
     * This method get the input as key value pair, which are the outputs of mapper group together
     * Again matches the key to reducer input data pattern and extract time interval and error count
     * Then outputs interval as key and error count as value in key-value pair
     * @param key - Text key which will be the same one that is passed in mapper's output
     * @param values - all the aggregated values that belong to a single key
     * @param output - output object which needs to written to
     * @param reporter - monitoring object
     * @throws IOException - IOException
     * @throws InterruptedException - InterruptedException
     */
    @throws[IOException]
    @throws[InterruptedException]
    override def reduce(key: Text, values: util.Iterator[NullWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      logger.info("reducer function called")
      val matcher = inputDataPattern.matcher(key.toString)

      // if the key matches "timeInterval,count" format, only then proceed to output the result
      if matcher.matches() then
        output.collect(new Text(matcher.group(1)), new IntWritable(matcher.group(2).toInt))


}
