/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.spark.monitoring.metrics;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.*;
import org.apache.spark.scheduler.cluster.ExecutorInfo;
import org.apache.wayang.spark.monitoring.interfaces.Stage;
import org.apache.wayang.spark.monitoring.interfaces.Task;
import org.apache.wayang.spark.monitoring.interfaces.*;
import scala.collection.Seq;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
/**
 * A Spark listener implementation that captures events generated during the Spark job execution
 * and sends them to a Kafka topic for further processing.
 */
public class SparkListener extends org.apache.spark.scheduler.SparkListener {
    // Member variables to store various data objects
    private List<Job> listOfJobs;
    private List<Stage> listOfStages;
    private List<Task> listOfTasks;
    private List<SerializableObject> applicationObjects;
    private List<SerializableObject> jobObjects;
    private List<SerializableObject> stageObjects;
    private List<SerializableObject> taskObjects;
    // Kafka producer to send data to Kafka topic
    private KafkaProducer<String, byte[]> producer;
    private final static String KAFKA_PROPERTIES = "/wayang-spark-kafka.properties";
    // Kafka topic name to which the data will be sent
    private String kafkaTopic;
    // Logger instance to log messages
    protected final Logger logger = LogManager.getLogger(this.getClass());
    /**
     * Default constructor that initializes the Kafka producer and various data lists.
     */
    public SparkListener(){
        Properties props = new Properties();
        try (InputStream inputStream = getClass().getResourceAsStream(KAFKA_PROPERTIES)) {
            props.load(inputStream);
        }
        catch (Exception e){
            logger.error("This is an error message with an exception.", e);
        }
        producer = new KafkaProducer<>(props);
        this.kafkaTopic = props.getProperty("kafka.topic");
        this.listOfJobs= new ArrayList<>();
        this.listOfStages= new ArrayList<>();
        this.listOfTasks= new ArrayList<>();
        this.applicationObjects= new ArrayList<>();
        this.jobObjects= new ArrayList<>();
        this.stageObjects= new ArrayList<>();
        this.taskObjects= new ArrayList<>();
    }

    @Override
    public void onExecutorBlacklisted(SparkListenerExecutorBlacklisted executorBlacklisted) {
        super.onExecutorBlacklisted(executorBlacklisted);
        executorBlacklisted.executorId();
        executorBlacklisted.time();
    }
    /**
     * Overridden method that captures the event generated when an executor is added in Spark
     * and sends it to the Kafka topic for further processing.
     *
     * @param executorAddedSpark The event that occurred when the executor was added
     */
    @Override
    public void onExecutorAdded(SparkListenerExecutorAdded executorAddedSpark) {
        super.onExecutorAdded(executorAddedSpark);
        Executor executorAdded= new ExecutorAdded();
         executorAdded.setEventame("ExecutorAdded");
         executorAdded.setExecutorID(executorAddedSpark.executorId());
        ExecutorInfo executorInfo=executorAddedSpark.executorInfo();
         executorAdded.setExecutorHost(executorInfo.executorHost());
         executorAdded.setTotalCores(executorInfo.totalCores());
        // executorAdded.setResourceInfo(executorInfo.resourceProfileId());
         executorAdded.setExecutorHost(executorInfo.executorHost());
         executorAdded.executorTime(executorAddedSpark.time());
        try {
            ByteArrayOutputStream boas = new ByteArrayOutputStream();
            ObjectOutput out = new ObjectOutputStream(boas);
            out.writeObject(executorAdded);
            producer.send(new ProducerRecord(kafkaTopic, "ExecutorAdded", boas.toByteArray()));
        }
        catch (Exception e){
            e.printStackTrace();
        }

    }

    /**

     This method is called when an executor is removed from a Spark application, and it sends information about
     the removal event to a Kafka topic.
     @param executorRemovedSpark the SparkListenerExecutorRemoved event containing information about the removed executor
     */
    @Override
    public void onExecutorRemoved(SparkListenerExecutorRemoved executorRemovedSpark) {
        super.onExecutorRemoved(executorRemovedSpark);
        Executor executorRemoved= new ExecutorRemoved();
         executorRemoved.setEventame("ExecutorRemoved");
         executorRemoved.setExecutorHost(executorRemovedSpark.executorId());
         executorRemoved.setReasonOfRemoval(executorRemovedSpark.reason());
         executorRemoved.executorTime(executorRemovedSpark.time());
        try {
            ByteArrayOutputStream boas = new ByteArrayOutputStream();
            ObjectOutput out = new ObjectOutputStream(boas);
            out.writeObject(executorRemoved);
            producer.send(new ProducerRecord(kafkaTopic, "ExecutorRemoved", boas.toByteArray()));
        }
        catch (Exception e){
            this.logger.error("Exception {} for executor added",e);
        }
    }
    /**

     This method is called when metrics are updated for an executor in a Spark application, and it sends information about
     the updated executor to a Kafka topic.
     @param executorMetricsUpdateSpark the SparkListenerExecutorMetricsUpdate event containing information about the updated executor's metrics
     */
    @Override
    public void onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate executorMetricsUpdateSpark) {
        super.onExecutorMetricsUpdate(executorMetricsUpdateSpark);
        Executor executorUpdated= new ExecutorUpdated();
         executorUpdated.setExecutorID(executorMetricsUpdateSpark.execId());
         executorUpdated.setEventame("ExecutorUpdated");
        try {
            ByteArrayOutputStream boas = new ByteArrayOutputStream();
            ObjectOutput out = new ObjectOutputStream(boas);
            out.writeObject(executorUpdated);
            producer.send(new ProducerRecord(kafkaTopic, "ExecutorUpdated", boas.toByteArray()));
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
    /**

     This method is called when a task starts in a Spark application, and it creates a new TaskStart object with information
     about the task and adds it to a list of task objects for serializable.
     @param taskStartSpark the SparkListenerTaskStart event containing information about the started task
     */
    @Override
    public void onTaskStart(SparkListenerTaskStart taskStartSpark) {
        super.onTaskStart(taskStartSpark);
        Task taskStart=new TaskStart();
        TaskInfo taskInfo= taskStartSpark.taskInfo();
        taskStart.setID(taskInfo.id());
        taskStart.setEventame("OnTaskStart");
        taskStart.setHostIP(taskInfo.host());
        taskStart.setStringExecutorID(taskInfo.executorId());
        taskStart.setTaskStatus(taskInfo.status());
        taskStart.setTaskID(taskInfo.taskId());
        taskStart.setIndex(taskInfo.index());
        taskStart.setLaunchTime(taskInfo.launchTime());
        taskStart.setFinishTime(taskInfo.finishTime());
     //this.taskStart.setDurationTime(taskInfo.duration());
        taskStart.setGettingTime(taskInfo.gettingResultTime());
        taskStart.setStageID(taskStartSpark.stageId());
        //this.taskStart.setPartition(taskInfo.);
        if(taskInfo.failed()){
          taskStart.setTaskStatusForRunning(Task.TaskStatusForRunning.FAILED);
        }
        else if(taskInfo.finished()){
           taskStart.setTaskStatusForRunning(Task.TaskStatusForRunning.FINISHED);
        }
        else if(taskInfo.killed()){
           taskStart.setTaskStatusForRunning(Task.TaskStatusForRunning.KILLED);
        }
        else  if(taskInfo.running()){
           taskStart.setTaskStatusForRunning(Task.TaskStatusForRunning.RUNNING);
        }
        else if(taskInfo.successful()){
            taskStart.setTaskStatusForRunning(Task.TaskStatusForRunning.SUCCESSFUL);
        }
        else if(taskInfo.speculative()){
           taskStart.setTaskStatusForRunning(Task.TaskStatusForRunning.SPECULATIVE);
        }
        else {
           taskStart.setTaskStatusForRunning(null);
        }

        this.taskObjects.add(taskStart);

    }

    /**
     * This method is called when a Spark application starts. It extends the behavior of the
     * superclass by passing along the given SparkListenerApplicationStart event to its parent
     * implementation.
     *
     * @param applicationStartSpark the SparkListenerApplicationStart event that was triggered
     */
    @Override
    public void onApplicationStart(SparkListenerApplicationStart applicationStartSpark) {
        super.onApplicationStart(applicationStartSpark);
        Application applicationStart= new ApplicationStart();
         applicationStart.setAppID(applicationStartSpark.appId().get());
         applicationStart.setEventame("ApplicationStart");
         applicationStart.setName(applicationStartSpark.appName());
         applicationStart.setSparkUser(applicationStartSpark.sparkUser());
         applicationStart.setStartTime(applicationStartSpark.time());
        this.applicationObjects.add((SerializableObject) applicationStart);
        // this.jobObjects.add((SerializableObject) this.applicationStart);
        // this.stageObjects.add((SerializableObject) this.applicationStart);
        // this.taskObjects.add((SerializableObject) this.applicationStart);
        // this.objects.add((SerializableObject) this.applicationStart);
    }
    /**

     This method is called when a new Spark job starts. It creates a new JobStart object to represent the event,
     sets its properties using the data provided in the SparkListenerJobStart object, and adds the object to the
     list of jobs and the list of job objects.
     @param jobStartSpark the SparkListenerJobStart object containing data about the new job
     */
    @Override
    public void onJobStart(SparkListenerJobStart jobStartSpark) {
        super.onJobStart(jobStartSpark);
        Job jobStart= new JobStart();
         jobStart.setEventame("JobStart");
         jobStart.setJobID(jobStartSpark.jobId());
         jobStart.setProductArity(jobStartSpark.productArity());
         jobStart.setStageID((Seq<Object>) jobStartSpark.stageIds());
        this.listOfJobs.add(jobStart);
        this.jobObjects.add((SerializableObject) jobStart);
    }
/**

 This method is called when a job ends in the Spark application. It creates a new instance of the JobEnd class,
 sets the necessary attributes and adds it to the list of jobs and job objects. Then it serializes the job objects
 and sends them to Kafka. It also resets the job objects and list of stages for the next job.
 @param jobEndSpark a SparkListenerJobEnd object representing the end of a job
 */
    @Override
    public void onJobEnd(SparkListenerJobEnd jobEndSpark) {
        super.onJobEnd(jobEndSpark);
        Job jobEnd= new JobEnd();
          jobEnd.setJobID(jobEndSpark.jobId());
          jobEnd.setEventame("JobEnd");
          jobEnd.setProductArity(jobEndSpark.productArity());
          jobEnd.setListOfStages(this.listOfStages);
        this.listOfJobs.add(jobEnd);
        this.jobObjects.add((SerializableObject) jobEnd);
        try {

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(this.jobObjects);
            producer.send(new ProducerRecord(kafkaTopic, "JobObjects", baos.toByteArray()));
            this.jobObjects= new ArrayList<>();
            this.listOfStages= new ArrayList<>();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    /**

     Called when a SparkListenerTaskEnd event is triggered.
     @param taskEndSpark The SparkListenerTaskEnd object representing the event that was triggered.
     This method overrides the onTaskEnd method from the superclass and performs additional actions:
     creates a new TaskEnd object and sets its properties based on the information in the taskEndSpark parameter
     adds the TaskEnd object to the listOfTasks and taskObjects arrays
     serializes the taskObjects array and sends it to a Kafka producer
     The TaskEnd object represents the end of a task, and includes information such as the task's ID, event name, host IP,
     executor ID, status, task ID, index, launch time, finish time, duration time, getting time, and task status for running.
     The TaskMetric object represents the metrics for the task, and includes information such as the executor CPU time,
     executor deserialize CPU time, executor deserialize time, disk bytes spilled, executor run time, JVM GC time, peak execution memory,
     result size, and result serialization time.
     This method catches and prints any exceptions that may occur during the serialization and sending of the taskObjects array.
     */

    @Override
    public void onTaskEnd(SparkListenerTaskEnd taskEndSpark) {
        super.onTaskEnd(taskEndSpark);
        Task taskEnd=new TaskEnd();
        TaskInfo taskInfo= taskEndSpark.taskInfo();
        taskEnd.setID(taskInfo.id());
        taskEnd.setEventame("OnTaskGettingResult");
        taskEnd.setHostIP(taskInfo.host());
        taskEnd.setStringExecutorID(taskInfo.executorId());
        taskEnd.setTaskStatus(taskInfo.status());
        taskEnd.setTaskID(taskInfo.taskId());
        taskEnd.setIndex(taskInfo.index());
        taskEnd.setLaunchTime(taskInfo.launchTime());
        taskEnd.setFinishTime(taskInfo.finishTime());
        taskEnd.setDurationTime(taskInfo.duration());
        taskEnd.setGettingTime(taskInfo.gettingResultTime());
        //  this. taskGettingResult.setStageID(taskGettingResult.stageId());
        //this.taskEnd.setPartition(taskInfo.partitionId());
        if(taskInfo.failed()){
          taskEnd.setTaskStatusForRunning(Task.TaskStatusForRunning.FAILED);
        }
        else if(taskInfo.finished()){
           taskEnd.setTaskStatusForRunning(Task.TaskStatusForRunning.FINISHED);
        }
        else if(taskInfo.killed()){
          taskEnd.setTaskStatusForRunning(Task.TaskStatusForRunning.KILLED);
        }
        else  if(taskInfo.running()){
           taskEnd.setTaskStatusForRunning(Task.TaskStatusForRunning.RUNNING);
        }
        else if(taskInfo.successful()){
           taskEnd.setTaskStatusForRunning(Task.TaskStatusForRunning.SUCCESSFUL);
        }
        else if(taskInfo.speculative()){
            taskEnd.setTaskStatusForRunning(Task.TaskStatusForRunning.SPECULATIVE);
        }
        else {
           taskEnd.setTaskStatusForRunning(null);
        }


        TaskMetrics taskMetrics= taskEndSpark.taskMetrics();
        TaskMetric taskMetric= new TaskMetric();
        taskMetric.setExecutorCPUTime(taskMetrics.executorCpuTime());
        taskMetric.setExecutorDeserializeCpuTime(taskMetrics.executorDeserializeCpuTime());
        taskMetric.setExecutorDeserializeTime(taskMetrics.executorDeserializeTime());
        taskMetric.setDiskBytesSpilled(taskMetrics.diskBytesSpilled());
        taskMetric.setExecutorRunTime(taskMetrics.executorRunTime());
        taskMetric.setjvmGCTime(taskMetrics.jvmGCTime());
        taskMetric.setPeakExecutionMemory(taskMetrics.peakExecutionMemory());
        taskMetric.setResultSize(taskMetrics.resultSize());
        taskMetric.setResultSerializationTime(taskMetrics.resultSerializationTime());
        taskEnd.setTaskMetric(taskMetric);
        this.listOfTasks.add(taskEnd);
        this.taskObjects.add(taskEnd);
        try {

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(this.taskObjects);
            producer.send(new ProducerRecord(kafkaTopic, "TaskEnd", baos.toByteArray()));
            this.taskObjects= new ArrayList<>();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
    /**

     Overrides the onStageCompleted method from SparkListener to customize handling of
     stage completion events. Creates a StageCompleted object and sets its properties based on the
     StageInfo and TaskMetrics of the completed stage. Adds the StageCompleted object to a list of stages
     and adds it to a list of SerializableObjects to be sent to a Kafka producer.
     @param stageCompletedSpark the SparkListenerStageCompleted event to be handled
     */
    @Override
    public void onStageCompleted(SparkListenerStageCompleted stageCompletedSpark) {
        super.onStageCompleted(stageCompletedSpark);
        Stage stageCompleted= new StageCompleted();
        StageInfo stageInfo=stageCompletedSpark.stageInfo();
        stageCompleted.setDetails(stageInfo.details());
        stageCompleted.setEventame("OnStageSubmitted");
        stageCompleted.setStageName(stageInfo.name());
         stageCompleted.setStatus(stageInfo.getStatusString());
         stageCompleted.setNumberOfTasks(stageInfo.numTasks());
         stageCompleted.setID(stageInfo.stageId());
         stageCompleted.setSubmissionTime((Long) stageInfo.submissionTime().get());
        stageCompleted.setCompletionTime((Long) stageInfo.completionTime().get());
        TaskMetrics taskMetrics= stageCompletedSpark.stageInfo().taskMetrics();
        TaskMetric taskMetric= new TaskMetric();
        taskMetric.setExecutorCPUTime(taskMetrics.executorCpuTime());
        taskMetric.setExecutorDeserializeCpuTime(taskMetrics.executorDeserializeCpuTime());
        taskMetric.setExecutorDeserializeTime(taskMetrics.executorDeserializeTime());
        taskMetric.setDiskBytesSpilled(taskMetrics.diskBytesSpilled());
        taskMetric.setExecutorRunTime(taskMetrics.executorRunTime());
        taskMetric.setjvmGCTime(taskMetrics.jvmGCTime());
        taskMetric.setPeakExecutionMemory(taskMetrics.peakExecutionMemory());
        taskMetric.setResultSize(taskMetrics.resultSize());
        taskMetric.setResultSerializationTime(taskMetrics.resultSerializationTime());
        stageCompleted.setTaskMetric(taskMetric);
        this.listOfStages.add(stageCompleted);
         stageCompleted.setListOfTasks(this.listOfTasks);
        this.stageObjects.add((SerializableObject) stageCompleted);
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(this.stageObjects);
            producer.send(new ProducerRecord(kafkaTopic, "Stage", baos.toByteArray()));
            this.stageObjects= new ArrayList<>();
            this.listOfTasks= new ArrayList<>();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    /**
     * This method is called whenever a new stage is submitted to the Spark engine. It adds the details of the
     * submitted stage to a list of stages and a list of stage objects.
     *
     * @param stageSubmittedSpark the SparkListenerStageSubmitted object containing information about the submitted stage
     */
    @Override
    public void onStageSubmitted(SparkListenerStageSubmitted stageSubmittedSpark) {
        super.onStageSubmitted(stageSubmittedSpark);
        Stage stageSubmitted= new StageSubmitted();
        StageInfo stageInfo=stageSubmittedSpark.stageInfo();
        stageSubmitted.setDetails(stageInfo.details());
        stageSubmitted.setEventame("OnStageSubmitted");
        stageSubmitted.setStageName(stageInfo.name());
        stageSubmitted.setStatus(stageInfo.getStatusString());
        stageSubmitted.setNumberOfTasks(stageInfo.numTasks());
        stageSubmitted.setID(stageInfo.stageId());
        stageSubmitted.setSubmissionTime((Long) stageInfo.submissionTime().get());
        //this.stageSubmitted.setCompletionTime((Long) stageInfo.completionTime().get());
        TaskMetrics taskMetrics= stageSubmittedSpark.stageInfo().taskMetrics();
        TaskMetric taskMetric= new TaskMetric();
        taskMetric.setExecutorCPUTime(taskMetrics.executorCpuTime());
        taskMetric.setExecutorDeserializeCpuTime(taskMetrics.executorDeserializeCpuTime());
        taskMetric.setExecutorDeserializeTime(taskMetrics.executorDeserializeTime());
        taskMetric.setDiskBytesSpilled(taskMetrics.diskBytesSpilled());
        taskMetric.setExecutorRunTime(taskMetrics.executorRunTime());
        taskMetric.setjvmGCTime(taskMetrics.jvmGCTime());
        taskMetric.setPeakExecutionMemory(taskMetrics.peakExecutionMemory());
        taskMetric.setResultSize(taskMetrics.resultSize());
        taskMetric.setResultSerializationTime(taskMetrics.resultSerializationTime());
        stageSubmitted.setTaskMetric(taskMetric);
        this.listOfStages.add(stageSubmitted);
        this.stageObjects.add((SerializableObject) stageSubmitted);

    }

    /**
     * This method is called when the Spark application ends. It creates a new ApplicationEnd object containing
     * the start time of the application and a list of jobs, and adds the object to a list of application objects.
     * It then serializes the list of application objects and sends it to a Kafka topic. Finally, it clears the
     * lists of application and job objects to prepare for the next application run.
     *
     * @param applicationEndSpark the SparkListenerApplicationEnd object containing information about the end of the application
     */
    @Override
    public void onApplicationEnd(SparkListenerApplicationEnd applicationEndSpark) {
        super.onApplicationEnd(applicationEndSpark);
        Application applicationEnd=new ApplicationEnd();
        applicationEnd.setStartTime(applicationEndSpark.time());
        applicationEnd.setListOfJobs(this.listOfJobs);
        this.applicationObjects.add((SerializableObject) applicationEnd);
        try {

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(this.applicationObjects);
            producer.send(new ProducerRecord(kafkaTopic, "ApplicationObjects", baos.toByteArray()));
            this.applicationObjects= new ArrayList<>();
            this.listOfJobs= new ArrayList<>();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
    /**

     Called when a task's result is being fetched. Adds a new Task object to the listOfTasks and taskObjects.
     @param taskGettingResultSpark The SparkListenerTaskGettingResult object containing information about the task result.
     */
    @Override
    public void onTaskGettingResult(SparkListenerTaskGettingResult taskGettingResultSpark) {
        super.onTaskGettingResult(taskGettingResultSpark);
        Task taskGettingResult=new TaskGettingResult();
        TaskInfo taskInfo= taskGettingResultSpark.taskInfo();
        taskGettingResult.setID(taskInfo.id());
        taskGettingResult.setEventame("OnTaskGettingResult");
        taskGettingResult.setHostIP(taskInfo.host());
        taskGettingResult.setStringExecutorID(taskInfo.executorId());
        taskGettingResult.setTaskStatus(taskInfo.status());
        taskGettingResult.setTaskID(taskInfo.taskId());
        taskGettingResult.setIndex(taskInfo.index());
        taskGettingResult.setLaunchTime(taskInfo.launchTime());
        taskGettingResult.setFinishTime(taskInfo.finishTime());
        taskGettingResult.setDurationTime(taskInfo.duration());
       taskGettingResult.setGettingTime(taskInfo.gettingResultTime());
        //  this. taskGettingResult.setStageID(taskGettingResult.stageId());
       // this.taskGettingResult.setPartition(taskInfo.partitionId());
        if(taskInfo.failed()){
           taskGettingResult.setTaskStatusForRunning(Task.TaskStatusForRunning.FAILED);
        }
        else if(taskInfo.finished()){
            taskGettingResult.setTaskStatusForRunning(Task.TaskStatusForRunning.FINISHED);
        }
        else if(taskInfo.killed()){
           taskGettingResult.setTaskStatusForRunning(Task.TaskStatusForRunning.KILLED);
        }
        else  if(taskInfo.running()){
           taskGettingResult.setTaskStatusForRunning(Task.TaskStatusForRunning.RUNNING);
        }
        else if(taskInfo.successful()){
           taskGettingResult.setTaskStatusForRunning(Task.TaskStatusForRunning.SUCCESSFUL);
        }
        else if(taskInfo.speculative()){
            taskGettingResult.setTaskStatusForRunning(Task.TaskStatusForRunning.SPECULATIVE);
        }
        else {
          taskGettingResult.setTaskStatusForRunning(null);
        }
        this.listOfTasks.add(taskGettingResult);
        this.taskObjects.add( taskGettingResult);

    }
}