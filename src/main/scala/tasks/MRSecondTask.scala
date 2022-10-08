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

object MRSecondTask {
  val configObject: Config = ConfigFactory.load().getConfig("mapReduceTasksConfig")

  class IntermediateMap extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable]:
    val logger: Logger = CreateLogger(classOf[IntermediateMap])

    val logPattern: Pattern = Pattern.compile(configObject.getString("LogPattern"))
    val stringMessagePattern: Pattern = Pattern.compile(configObject.getString("StringMessagePattern"))

    @throws[IOException]
    @throws[InterruptedException]
    override def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val intervalInSeconds: Int = configObject.getInt("MRSecondTaskIntervalInSeconds")

      val matcher = logPattern.matcher(value.toString)
      if(matcher.matches()) {
        val stringInstanceMatcher = stringMessagePattern.matcher(matcher.group(5))

        if matcher.group(3) == configObject.getString("MessageTypeError") && stringInstanceMatcher.matches() then
          val (timeStartString, timeEndString) = TimeUtil.convertToHourMinuteInterval(matcher.group(1), intervalInSeconds)
          output.collect(new Text(timeStartString.concat("-").concat(timeEndString)), new IntWritable(1))

      }


  class IntermediateReduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable]:
    val logger: Logger = CreateLogger(classOf[IntermediateReduce])

    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val sum = values.asScala.reduce((valueOne, valueTwo) => IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key, new IntWritable(sum.get()))

  class SortComparator() extends WritableComparator(classOf[Text], true):
    override def compare(a: WritableComparable[_], b: WritableComparable[_]): Int =
      val textAArray = a.asInstanceOf[Text].toString.split(configObject.getString("OutputFormatSeparator"))
      val textBArray = b.asInstanceOf[Text].toString.split(configObject.getString("OutputFormatSeparator"))
      val comparison = textAArray(1).toInt.compareTo(textBArray(1).toInt)
      if comparison == 0 then
        -1 * textAArray(0).compareTo(textBArray(0))
      else
        -1 * comparison

  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, NullWritable]:
    val inputDataPattern: Pattern = Pattern.compile(configObject.getString("IntermediateTaskOutputPattern"))
    val nullVal: NullWritable = NullWritable.get()

    override def map(key: LongWritable, value: Text, output: OutputCollector[Text, NullWritable], reporter: Reporter): Unit =
      val matcher = inputDataPattern.matcher(value.toString)

      if matcher.matches() then
        output.collect(value, nullVal)

  class Reduce extends MapReduceBase with Reducer[Text, NullWritable, Text, IntWritable]:
    val inputDataPattern: Pattern = Pattern.compile(configObject.getString("IntermediateTaskOutputPattern"))

    override def reduce(key: Text, values: util.Iterator[NullWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val matcher = inputDataPattern.matcher(key.toString)
      if matcher.matches() then
        output.collect(new Text(matcher.group(1)), new IntWritable(matcher.group(2).toInt))


}
