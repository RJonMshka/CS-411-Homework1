package tasks

import scala.jdk.CollectionConverters.*
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapred.{MapReduceBase, Mapper, OutputCollector, Reducer, Reporter}
import org.slf4j.Logger
import tasks.HelperUtils.{CreateLogger, OtherUtils}
import tasks.MRFirstTask.configObject
import tasks.MRThirdTask.{Map, configObject}

import java.io.IOException
import java.util
import java.util.regex.Pattern


/**
 * This object is a wrapper for Mapper and Reducer of fourth task
 */
object MRFourthTask {
  val configObject: Config = ConfigFactory.load().getConfig("mapReduceTasksConfig")

  /**
   *  Mapper class for fourth task
   */
  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, Text]:
    val logPattern: Pattern = Pattern.compile(configObject.getString("LogPattern"))
    val stringMessagePattern: Pattern = Pattern.compile(configObject.getString("StringMessagePattern"))
    val logger: Logger = CreateLogger(classOf[Map])

    /**
     * This method matches the log message to the log pattern, if matches, writes to output with log message type as key and log message length and string instance length encoded together as value
     * @param key - LongWritable object - we do not use this key anywhere, however this is the key for a single entry to map method
     * @param value - a single log text
     * @param output - output object which needs to written to
     * @param reporter - monitoring object
     * @throws IOException
     * @throws InterruptedException
     */
    @throws[IOException]
    @throws[InterruptedException]
    override def map(key: LongWritable, value: Text, output: OutputCollector[Text, Text], reporter: Reporter): Unit =
      logger.info("map function called")
      val matcher = logPattern.matcher(value.toString)

      if matcher.matches() then
        output.collect(new Text(matcher.group(3)), new Text(OtherUtils.encode( matcher.group(0).length.toString, matcher.group(5).length.toString )))

  /**
   * Class for Reducer of first task
   */
  class Reduce extends MapReduceBase with Reducer[Text, Text, Text, IntWritable]:
    val logger: Logger = CreateLogger(classOf[Reduce])

    def getMaxCharacter(text: Text, previousMax: (Int, Int)): (Int, Int) =
      val (logMessageLength: String, patternInstanceLength: String) = OtherUtils.decode(text.toString)
      if patternInstanceLength.toInt > previousMax._2 then (logMessageLength.toInt, patternInstanceLength.toInt) else previousMax

    /**
     * This method get the input as key value pair, which are the outputs of mapper group together
     * Finds the max of values for a single key and then outputs key, value pair
     * @param key - Text key which will be the same one that is passed in mapper's output
     * @param values - all the aggregated values that belong to a single key
     * @param output - output object which needs to written to
     * @param reporter - monitoring object
     * @throws IOException
     * @throws InterruptedException
     */
    @throws[IOException]
    @throws[InterruptedException]
    override def reduce(key: Text, values: util.Iterator[Text], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      logger.info("reducer function called")
      val (maxCharLength, _) = values.asScala.foldRight((0, 0))(getMaxCharacter)
      output.collect(key, IntWritable(maxCharLength))
}
