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

object MRFourthTask {
  val configObject: Config = ConfigFactory.load().getConfig("mapReduceTasksConfig")


  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, Text]:
    val logPattern: Pattern = Pattern.compile(configObject.getString("LogPattern"))
    val stringMessagePattern: Pattern = Pattern.compile(configObject.getString("StringMessagePattern"))
    val logger: Logger = CreateLogger(classOf[Map])

    @throws[IOException]
    @throws[InterruptedException]
    override def map(key: LongWritable, value: Text, output: OutputCollector[Text, Text], reporter: Reporter): Unit =
      val matcher = logPattern.matcher(value.toString)

      if matcher.matches() then
        output.collect(new Text(matcher.group(3)), new Text(OtherUtils.encode( matcher.group(0).length.toString, matcher.group(5).length.toString )))

  class Reduce extends MapReduceBase with Reducer[Text, Text, Text, IntWritable]:

    def getMaxCharacter(text: Text, previousMax: (Int, Int)): (Int, Int) =
      val (logMessageLength: String, patternInstanceLength: String) = OtherUtils.decode(text.toString)
      if patternInstanceLength.toInt > previousMax._2 then (logMessageLength.toInt, patternInstanceLength.toInt) else previousMax

    override def reduce(key: Text, values: util.Iterator[Text], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val (maxCharLength, _) = values.asScala.foldRight((0, 0))(getMaxCharacter)
      output.collect(key, IntWritable(maxCharLength))
}
