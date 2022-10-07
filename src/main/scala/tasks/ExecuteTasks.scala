package tasks

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.{LocalFileSystem, Path}
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapred.{FileInputFormat, FileOutputFormat, JobClient, JobConf, TextInputFormat, TextOutputFormat}

object ExecuteTasks {
  val configReference = ConfigFactory.load().getConfig("mapReduceTasksConfig")

  private def setCommonConfigSettings(config: JobConf): Unit =
    config.set("mapreduce.output.textoutputformat.separator", configReference.getString("OutputFormatSeparator"))
    config.set("fs.hdfs.impl", classOf[DistributedFileSystem].getName)
    config.set("fs.file.impl", classOf[LocalFileSystem].getName)

  private def executeFirstTask(inputPath: String, outputPath: String): Unit =
    val conf = new JobConf(classOf[MRFirstTask.type])
    conf.setJobName(configReference.getString("FirstTaskJobName"))
    this.setCommonConfigSettings(conf)
    conf.set("mapreduce.job.maps", configReference.getString("MRFirstTaskMapperCount"))
    conf.set("mapreduce.job.reduces", configReference.getString("MRFirstTaskReducerCount"))
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])
    conf.setMapperClass(classOf[MRFirstTask.Map])
    conf.setCombinerClass(classOf[MRFirstTask.Reduce])
    conf.setReducerClass(classOf[MRFirstTask.Reduce])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.setInputPaths(conf, new Path(inputPath))
    FileOutputFormat.setOutputPath(conf, new Path(outputPath))
    JobClient.runJob(conf)

  private def executeThirdTask(inputPath: String, outputPath: String): Unit =
    val conf = new JobConf(classOf[MRThirdTask.type])
    conf.setJobName(configReference.getString("ThirdTaskJobName"))
    this.setCommonConfigSettings(conf)
    conf.set("mapreduce.job.maps", configReference.getString("MRThirdTaskMapperCount"))
    conf.set("mapreduce.job.reduces", configReference.getString("MRThirdTaskReducerCount"))
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])
    conf.setMapperClass(classOf[MRThirdTask.Map])
    conf.setCombinerClass(classOf[MRThirdTask.Reduce])
    conf.setReducerClass(classOf[MRThirdTask.Reduce])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.setInputPaths(conf, new Path(inputPath))
    FileOutputFormat.setOutputPath(conf, new Path(outputPath))
    JobClient.runJob(conf)

  private def executeFourthTask(inputPath: String, outputPath: String): Unit =
    val conf = new JobConf(classOf[MRFourthTask.type])
    conf.setJobName(configReference.getString("FourthTaskJobName"))
    this.setCommonConfigSettings(conf)
    conf.set("mapreduce.job.maps", configReference.getString("MRFourthTaskMapperCount"))
    conf.set("mapreduce.job.reduces", configReference.getString("MRFourthTaskReducerCount"))
    conf.setMapOutputKeyClass(classOf[Text])
    conf.setMapOutputValueClass(classOf[Text])
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])
    conf.setMapperClass(classOf[MRFourthTask.Map])
    conf.setReducerClass(classOf[MRFourthTask.Reduce])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.setInputPaths(conf, new Path(inputPath))
    FileOutputFormat.setOutputPath(conf, new Path(outputPath))
    JobClient.runJob(conf)

  @main def runTasks(taskType: String, inputPath: String, outputPath: String): Unit =
    if(taskType == this.configReference.getString("ExecuteFirstTask")) then
      this.executeFirstTask(inputPath, outputPath)
    else if(taskType == this.configReference.getString("ExecuteThirdTask")) then
      this.executeThirdTask(inputPath, outputPath)
    else if(taskType == this.configReference.getString("ExecuteFourthTask")) then
      this.executeFourthTask(inputPath, outputPath)
}
