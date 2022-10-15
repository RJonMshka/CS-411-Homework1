# CS-441 Cloud Computing Objects
## Homework 1 Documentation
## By: Rajat Kumar (UIN: 653922910)

---

## Introduction
This homework involves demonstration of hadoop framework to perform map reduce tasks using distributive computing. 
All the tasks that belong to this homework are focused on extracting meaningful data from log messages. These log messages 
are generated using a Random message generator script. The whole idea of this project is to read tons of data full of log messages, 
filter the log messages that match a particular pattern and then give various insight into this data by outputting a concise CSV data format output.
More details about implementation, data semantics and deployment are given in later sections of this documentation.

## How to run the application
1. Download IntelliJ or your favourite IDE. The application is developed using IntelliJ IDE, and it is highly recommended to use it for various reasons.
2. Make sure you have Java SDK version 18 installed on your machine. 
3. Also, it is assumed that your machine have git (version control) installed on your machine. If not, please do.
4. Clone this repository from GitHub and switch to `main` branch. This is where the latest code is located.
5. Open IntelliJ, and open up this project in the IDE environment. Or you can do `New - Project from Version Control` and then enter the GitHub URL of this repository to load in directly into your system if not cloned already.
6. The application's code is written using Scala programming language. The version used for Scala is `3.1.3`.
7. For building the project, `sbt` (Scala's Simple Build Tool) is used. Version `1.7.2` is used for sbt.
8. All the dependencies and build setting can be found in `build.sbt` file.
9. Once intelliJ is successfully detected the Scala, sbt, and right versions for both of them, the project is ready to be compiled/built.
10. Go to terminal at the bottom of IntelliJ, or open any terminal with path being set to the project's root directory.
11. Enter `sbt clean compile`, the project will start building.
12. Test cases are written using a library called `ScalaTest`.
13. All the test cases are in `src/test/scala/MRTasksTest.scala` file. You can even add your own test cases to test the application even more.
14. You can run the tests using `sbt test` to see the if all test cases pass or not. As of the moment this documentation is written and probably the time when you will be testing, all the test cases will pass (hopefully).
15. Finally, run `sbt assembly` or `sbt clean compile assembly` if you want to merge the compilation and generating fat jar step together.
16. The `sbt assembly` command will generate a fat jar file that will contain your application's byte code as well as the code from dependencies that the application is importing.
17. The jar file will be generating in `target/scala-3.1.3/` directory with name `Homework1-assembly-1.0.0.jar`.
18. This jar file can be run on local machine if your machine has hadoop installed on it. 
19. To do that, first copy the input log files from `src/main/resources/input/` into a directory on hdfs.
20. Then perform `hadoop jar` command to run the various tasks for this application. Make sure you have hadoop installed and running in your system.
21. The correct syntax or format of running every task is provided in further sections.
22. This jar file can also be deployed to AWS EMR (Elastic MapReduce) cluster. The steps for that will be cover in a later section.
23. That's pretty much it for running the application, please experiment around with the application.

---

## How to Deploy to AWS EMR
The deployment of this application to AWS EMR is done and the steps can be viewed in this ([video link](https://www.youtube.com/watch?v=sc2vCEfdQpU)) YouTube video.
The documentation for AWS EMR can be found [here](https://docs.aws.amazon.com/emr/index.html).

---
## Implementation Details
All the 4 tasks are implemented with one single entry point, the main method `tasks.ExecuteTasks.runTasks`.
This method takes 4 string parameters. The first is used to define the task. For example, 1 means first task, 2 means second, 3 means third and 4 means 4th task.
The next two parameters will be used as input and output respectively for all tasks except second task.
For second task, second parameter is input path, 3rd parameter is the intermediate output path and the fourth parameter is the final output path.
For tasks other than second, the fourth parameter can be anything, but it needs to be passed. 
Mappers and Reducers for each task are separated in their respective object files.
There us a `HelperUtils` package which contain common utilities like logger, config reference object, time utils and other common utils.
All the configurations for application can be found at `src/main/resources/application.conf` file.
The configuration for logback logging framework used can be found at `src/main/resources/logback.xml` file.

There are some sample outputs of the task in `src/main/resources/sampleOutputs` directory.

Also, to test the application with real input data, the data is present at `src/main/resources/input` directory. This data can be copied to HDFS or S3 bucket (in case of deploying on AWS EMR) and can be analyzed using hadoop's mapreduce framework.

---
## Various Map-Reduce Tasks and how to run them

### Task 1
Summary - This task's input will be start time and end time in format "HH:mm:ss.SSS".
If any log message having time within this range will be matched with string pattern.
The result of this task would be show how many log messages of each type (Error, Warn, Info, Debug) are matched that belong to that time range or period.

The input start time and end time is provided from "application.conf" file.

Run command: `hadoop jar "Path-to-jar-file" 1 "Path-to-input-folder-that-contains-input-log-data" "Path-to-output-folder" 0`

Example: `hadoop jar C:/Homework1-assembly-1.0.0.jar 1 /user/input/ /user/output/ 0`

**Note: There is a "0" last argument to the above command. This argument can be anything, but it needs to there as the main function expects it.
This argument is only meant to be used for task 2. 
Also, the argument after the path of jar file is "1" which specifies the task number.**

Sample output for task 1 looks like one below:

```
DEBUG,1324
ERROR,56
INFO,9304
WARN,1126
```

This means there are 1324 log message with debug level whose string message matched a particular pattern. 
The same goes for other levels as well.

### Task 2
Summary - This task's input will be a time interval in seconds (provided from config file). 
This task calculates the number of "ERROR" level log message for each fixed time interval in log messages in descending order.

Run Command: `hadoop jar "Path-to-jar-file" 2 "Path-to-input-folder-that-contains-input-log-data" "Path-to--intermediate-output-folder" "Path-to-output-folder"`

Example: `hadoop jar C:/Homework1-assembly-1.0.0.jar 1 /user/input/ /user/intermediateOutput/ /user/output/`

**Note: In this task, an intermediate output folder is used. This is because, the task 2 implemented using two jobs, first job
outputs the result in the intermediate output folder.
Then second job will take the result of first job from intermediate output folder as input and outputs the final result in the output folder.
Also, the argument after the path of jar file is "2" which specifies the task number.**

Sample output for task 2 looks like one below:

```
08:24:00-08:25:00,45
08:25:00-08:26:00,38
09:04:00-09:05:00,33
09:06:00-09:07:00,28
09:05:00-09:06:00,28
08:55:00-08:56:00,28
08:23:00-08:24:00,28
08:54:00-08:55:00,25
```

The above is calculated for time interval "60" seconds by specifying it in config file.

The first entry says that between 08:24:00 and 08:25:00 timestamps (60 second difference), there are 45 error messages found and so on.
And you can also see that the listing is arranged in descending order of number of error messages.

## Task 3
Summary - This task calculates how many log messages belong to each log type.

Run command: `hadoop jar "Path-to-jar-file" 3 "Path-to-input-folder-that-contains-input-log-data" "Path-to-output-folder" 0`

Example: `hadoop jar C:/Homework1-assembly-1.0.0.jar 3 /user/input/ /user/output/ 0`

**Note: There is a "0" last argument to the above command. This argument can be anything, but it needs to there as the main function expects it.
This argument is only meant to be used for task 2.
Also, the argument after the path of jar file is "3" which specifies the task number.**

Sample output for task 3 looks like one below:

```
DEBUG,19856
ERROR,2152
INFO,139956
WARN,38048
```
This sample output represents that total of 19856 debug level messages are found in all input logs. The same goes for other log levels.

## Task 4
Summary - This task computes the length of log message which has maximum character length of the matched string message with the string pattern for each log type.

Run command: `hadoop jar "Path-to-jar-file" 4 "Path-to-input-folder-that-contains-input-log-data" "Path-to-output-folder" 0`

Example: `hadoop jar C:/Homework1-assembly-1.0.0.jar 4 /user/input/ /user/output/ 0`

**Note: There is a "0" last argument to the above command. This argument can be anything, but it needs to there as the main function expects it.
This argument is only meant to be used for task 2.
Also, the argument after the path of jar file is "4" which specifies the task number.**

Sample output for task 4 looks like one below:

```
DEBUG,166
ERROR,105
INFO,74
WARN,125
```

This means that for DEBUG level, the log message with the largest character length of the string instance has a length og 166 characters.
The same is true for other debug levels.

For example, the below is a log message of type INFO.
```
23:20:40.781 [scala-execution-context-global-17] INFO  HelperUtils.Parameters$ - T)agkrWh6>&m)pEp7NR
```

Let us assume that the string message `T)agkrWh6>&m)pEp7NR` has the biggest character length among all other INFO level logs and `23:20:40.781 [scala-execution-context-global-17] INFO  HelperUtils.Parameters$ - T)agkrWh6>&m)pEp7NR` has length 100.
So, the program will return `INFO, 100` as its output. That's how the 4th task is implemented.

It could have been implemented the easier way by just returning the max string pattern instance length (string message length). But I chose otherwise.

---
### That's all. Thank you so much.