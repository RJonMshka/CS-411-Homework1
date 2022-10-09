package tasks

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.{LocalFileSystem, Path}
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.hadoop.io.{IntWritable, LongWritable, NullWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapred.{FileInputFormat, FileOutputFormat, JobClient, JobConf, TextInputFormat, TextOutputFormat}
import org.slf4j.Logger
import tasks.HelperUtils.CreateLogger

/**
 * Object responsible for execution of the application
 * This object's main method will be the entry point of the application
 */
object ExecuteTasks {
  val configReference: Config = ConfigFactory.load().getConfig("mapReduceTasksConfig")
  val logger: Logger = CreateLogger(classOf[ExecuteTasks.type])

  /**
   * This method is used to set the common configurations MapReduce jobs
   * @param config - configuration object which need to be configured
   */
  private def setCommonConfigSettings(config: JobConf): Unit =
    config.set("mapreduce.output.textoutputformat.separator", configReference.getString("OutputFormatSeparator"))
    config.set("fs.hdfs.impl", classOf[DistributedFileSystem].getName)
    config.set("fs.file.impl", classOf[LocalFileSystem].getName)

  /**
   * Executes first task
   * @param inputPath - path to input files
   * @param outputPath - path to output
   */
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

  /**
   * Executes third task
   * @param inputPath - path to input files
   * @param outputPath - path to output
   */
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

  /**
   * Executes fourth task
   * @param inputPath - path to input files
   * @param outputPath - path to output
   */
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

  /**
   * Executes intermediate second task
   * @param inputPath - path to input files
   * @param intermediateOutputPath - path to intermediate output
   * @param outputPath - path to output
   */
  private def executeIntermediateSecondTask(inputPath: String, intermediateOutputPath: String, outputPath: String): Unit =
    val conf = new JobConf(classOf[MRFirstTask.type])
    conf.setJobName(configReference.getString("SecondIntermediateTaskJobName"))
    this.setCommonConfigSettings(conf)
    conf.set("mapreduce.job.maps", configReference.getString("MRSecondIntermediateTaskMapperCount"))
    conf.set("mapreduce.job.reduces", configReference.getString("MRSecondIntermediateTaskReducerCount"))
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])
    conf.setMapperClass(classOf[MRSecondTask.IntermediateMap])
    conf.setReducerClass(classOf[MRSecondTask.IntermediateReduce])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.setInputPaths(conf, new Path(inputPath))
    FileOutputFormat.setOutputPath(conf, new Path(intermediateOutputPath))
    val runningJob = JobClient.runJob(conf)
    // Waiting for intermediate job to complete
    runningJob.waitForCompletion()
    if runningJob.isSuccessful then
      logger.info("the intermediate job finished successfully, starting the final job")
      this.executeSecondFinalTask(intermediateOutputPath, outputPath)
    else
      logger.error("The intermediate job failed, unable to start the final job")

  /**
   * Executes final second task
   * @param inputPath - path to input files (intermediate output)
   * @param outputPath - path to output
   */
  private def executeSecondFinalTask(inputPath: String, outputPath: String): Unit =
    val conf = new JobConf(classOf[MRFirstTask.type])
    conf.setJobName(configReference.getString("SecondFinalTaskJobName"))
    this.setCommonConfigSettings(conf)
    conf.set("mapreduce.job.maps", configReference.getString("MRSecondFinalTaskMapperCount"))
    conf.set("mapreduce.job.reduces", configReference.getString("MRSecondFinalTaskReducerCount"))
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])
    conf.setMapperClass(classOf[MRSecondTask.Map])
    conf.setMapOutputKeyClass(classOf[Text])
    conf.setMapOutputValueClass(classOf[NullWritable])
    conf.setReducerClass(classOf[MRSecondTask.Reduce])
    conf.setOutputKeyComparatorClass(classOf[MRSecondTask.SortComparator])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.setInputPaths(conf, new Path(inputPath))
    FileOutputFormat.setOutputPath(conf, new Path(outputPath))
    JobClient.runJob(conf)

  /**
   * Main method - entry point of application
   * This method decides which task to execute based on parameters passed
   * @param taskType - which task that needs to be executed
   * @param inputPath - path to input shards
   * @param outputPath - output path to all tasks except 2nd one, for 2nd task, this will be an intermediate output path
   * @param nextOutputPath - final output path for 2nd task, and should be passed anything else for other tasks
   */
  @main def runTasks(taskType: String, inputPath: String, outputPath: String, nextOutputPath: String): Unit =
    if taskType == this.configReference.getString("ExecuteFirstTask") then
      logger.info("Starting First Task")
      this.executeFirstTask(inputPath, outputPath)
    else if taskType == this.configReference.getString("ExecuteSecondTask") then
      logger.info("Starting Second Task")
      this.executeIntermediateSecondTask(inputPath, outputPath, nextOutputPath)
    else if taskType == this.configReference.getString("ExecuteThirdTask") then
      logger.info("Starting Third Task")
      this.executeThirdTask(inputPath, outputPath)
    else if taskType == this.configReference.getString("ExecuteFourthTask") then
      logger.info("Starting Fourth Task")
      this.executeFourthTask(inputPath, outputPath)
    else
      logger.error("Unable to find appropriate job given the arguments")
}
