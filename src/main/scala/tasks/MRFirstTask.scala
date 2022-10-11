package tasks

import scala.jdk.CollectionConverters.*
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapred.{FileInputFormat, FileOutputFormat, JobClient, JobConf, MapReduceBase, Mapper, OutputCollector, Reducer, Reporter, TextInputFormat, TextOutputFormat}
import org.slf4j.Logger
import tasks.HelperUtils.{CreateLogger, TimeUtil}

import java.io.IOException
import java.{lang, util}
import java.util.regex.Pattern


/**
 * This object is a wrapper for Mapper and Reducer of first task
 */
object MRFirstTask:
  // get the config reference
  val configObject: Config = ConfigFactory.load().getConfig("mapReduceTasksConfig")

  /**
   *  Mapper class for first task
   */
  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable]:
    val logger: Logger = CreateLogger(classOf[Map])
    val logPattern: Pattern = Pattern.compile(configObject.getString("LogPattern"))
    val stringMessagePattern: Pattern = Pattern.compile(configObject.getString("StringMessagePattern"))
    val startTime: String = configObject.getString("MRFirstTaskStartInterval")
    val endTime: String = configObject.getString("MRFirstTaskEndInterval")
    // time difference in milliseconds between start time and end time
    val timeDifference: Long = TimeUtil.getInterval(startTime, endTime)

    /**
     * This method matches the log message to the log matcher and then checks
     * if the time of log message is within the provided interval or not
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
      val matcher = logPattern.matcher(value.toString)

      // proceed only if the log message matches the log pattern regex
      if matcher.matches() then
        val currentTimeDifference = TimeUtil.getInterval(startTime, matcher.group(1))
        val logMessageMatcher = stringMessagePattern.matcher(matcher.group(5))

        // if the time difference between current log message is within the provided time range and if the string message is an instance of the pattern to generate log messages, then only proceed
        if (currentTimeDifference >= 0L && currentTimeDifference <= timeDifference) && logMessageMatcher.matches() then
          output.collect(new Text(matcher.group(3)), new IntWritable(1))

  /**
   * Class for Reducer of first task
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
     * @throws IOException
     * @throws InterruptedException
     */
    @throws[IOException]
    @throws[InterruptedException]
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      logger.info("Reducer function called")
      val sum = values.asScala.reduce((valueOne, valueTwo) => IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key, new IntWritable(sum.get()))





