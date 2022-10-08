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

object MRFirstTask:
  val configObject: Config = ConfigFactory.load().getConfig("mapReduceTasksConfig")

  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable]:
    val logger: Logger = CreateLogger(classOf[Map])
    val logPattern: Pattern = Pattern.compile(configObject.getString("LogPattern"))
    val stringMessagePattern: Pattern = Pattern.compile(configObject.getString("StringMessagePattern"))
    val startTime: String = configObject.getString("MRFirstTaskStartInterval")
    val endTime: String = configObject.getString("MRFirstTaskEndInterval")
    val timeDifference: Long = TimeUtil.getInterval(startTime, endTime)

    @throws[IOException]
    @throws[InterruptedException]
    override def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val matcher = logPattern.matcher(value.toString)
      if matcher.matches() then
        val currentTimeDifference = TimeUtil.getInterval(startTime, matcher.group(1))
        val logMessageMatcher = stringMessagePattern.matcher(matcher.group(5))
        if currentTimeDifference >= 0L && currentTimeDifference <= timeDifference then
          if logMessageMatcher.matches() then
            output.collect(new Text(matcher.group(3)), new IntWritable(1))
          else 
            output.collect(new Text(matcher.group(3)), new IntWritable(0))
          
        
      

  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable]:
    val logger: Logger = CreateLogger(classOf[Reduce])

    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val sum = values.asScala.reduce((valueOne, valueTwo) => IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key, new IntWritable(sum.get()))





