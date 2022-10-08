package tasks

import scala.jdk.CollectionConverters.*
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapred.{FileInputFormat, FileOutputFormat, JobClient, JobConf, MapReduceBase, Mapper, OutputCollector, Reducer, Reporter, TextInputFormat, TextOutputFormat}
import tasks.HelperUtils.{CreateLogger, TimeUtil}

import java.io.IOException
import java.{lang, util}
import java.util.regex.Pattern

object MRFirstTask:
  val configObject = ConfigFactory.load().getConfig("mapReduceTasksConfig")

  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable]:
    val logger = CreateLogger(classOf[Map])
    val logPattern = Pattern.compile(configObject.getString("LogPattern"))
    val stringMessagePattern = Pattern.compile(configObject.getString("StringMessagePattern"))
    val startTime = configObject.getString("MRFirstTaskStartInterval")
    val endTime = configObject.getString("MRFirstTaskEndInterval")
    val timeDifference = TimeUtil.getInterval(startTime, endTime)

    @throws[IOException]
    @throws[InterruptedException]
    override def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val matcher = logPattern.matcher(value.toString)
      if(matcher.matches()) {
        val currentTimeDifference = TimeUtil.getInterval(startTime, matcher.group(1))
        val logMessageMatcher = stringMessagePattern.matcher(matcher.group(5))
        if(currentTimeDifference >= 0L && currentTimeDifference <= timeDifference) {
          if(logMessageMatcher.matches()) {
            output.collect(new Text(matcher.group(3)), new IntWritable(1))
          } else {
            output.collect(new Text(matcher.group(3)), new IntWritable(0))
          }
        }
      }

  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable]:
    val logger = CreateLogger(classOf[Reduce])

    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val sum = values.asScala.reduce((valueOne, valueTwo) => IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key, new IntWritable(sum.get()))





