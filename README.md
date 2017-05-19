# RedJob [![Build Status](https://travis-ci.org/shopping24/redjob.svg?branch=master)](https://travis-ci.org/shopping24/redjob)

[Spring](http://projects.spring.io/spring-data-redis/) based implementation 
of a [Redis](https://redis.io/) backed job queue.

RedJob is based on the ideas of [Resque](https://github.com/resque/resque) 
and [Jesque](https://github.com/gresrun/jesque), but is not compatible with them.

## Build the project
To build (clean/compile/test/install) the project, invoke

    $ mvn clean install
    
## Build a release
To publish a release to maven central, invoke

    # Create release and tag it in git:
    $ mvn release:prepare
    # Deploy it to maven central:
    $ mvn release:perform

## Usage

### Context

Setup of a basic environment for executing jobs, described in Spring XML context syntax:

```xml
<!-- Setup Redis data source. -->
<bean id="jobRedis" class="org.springframework.data.redis.connection.jedis.JedisConnectionFactory">
  <property name="hostName" value="localhost" />
  <property name="port" value="6379" />
  <property name="database" value="0" />
  <property name="timeout" value="2000" />
</bean>

<!-- Configure JSON serialization. -->
<bean id="executions" class="com.s24.redjob.worker.json.ExecutionRedisSerializer" />
<!-- Search job class in classpath. -->
<bean id="typeScanner" class="com.s24.redjob.worker.json.TypeScanner"
    p:executions-ref="executions"
    p:basePackages="com.s24.myapp" />
<!-- Factory for creating job runners for jobs. -->     
<bean id="jobRunnerFactory" class="com.s24.redjob.worker.InterfaceJobRunnerFactory" />

<!-- DAO for worker states. -->
<bean id="workerDao" class="com.s24.redjob.worker.WorkerDaoImpl"
    p:connectionFactory-ref="jobRedis"
    p:namespace="mynamespace" />

<!-- DAO for queues. -->
<bean id="fifoDao" class="com.s24.redjob.queue.FifoDaoImpl"
    p:connectionFactory-ref="jobRedis"
    p:namespace="mynamespace"
    p:executions-ref="executions" />

<!-- Worker polling the queue and executing jobs. -->
<bean id="defaultWorker" class="com.s24.redjob.queue.FifoWorkerFactoryBean"
    p:workerDao-ref="workerDao"
    p:name="myworker:[hostname]"
    p:fifoDao-ref="fifoDao"
    p:queues="myqueue"
    p:jobRunnerFactory-ref="jobRunnerFactory" />

<!-- Client, e.g. for adding jobs. -->
<bean id="redjobClient" class="com.s24.redjob.client.ClientFactoryBean"
    p:connectionFactory-ref="jobRedis"
    p:namespace="mynamespace"
    p:executions-ref="executions" />
```

### Jobs, runners and factories

Every bean that can be serialized to JSON by [Jackson](https://github.com/FasterXML/jackson) can be a job.
To execute a job, a job runner is required. Creating job runners is done by a JobRunnerFactory.
The easiest version of a JobRunnerFactory is the InterfaceJobRunnerFactory.
For this factory to work, the job runners need to implement the JobRunner<J> interface.
The factory evaluates the type parameter J used by the concrete JobRunner implementation 
to determine for which job the job runner is for.

### Example

Assuming you have a Spring application context configured like described above,
here are some very basic classes to create a RedJob "Hello world!":

```java
@JsonTypeName
public class HelloWorld {
   public String message = "Hello world!";
}

@JobRunnerComponent
public class HelloWorldJobRunner implements JobRunner<HelloWorld> {
   @Override
   public void execute(HelloWorld helloWord) {
      System.out.println(helloWord.message);      
   }
}

@Service
public class Sender {
   @Autowired
   private Client client;
   
   public void send() {
      client.enqueue("myqueue", new HelloWorld());
   }
}
```

You need a local Redis to make the example work:

```shell
redis-server
```

Calling the `send` method on `Sender` adds a `HelloWorld` job to the `myqueue` queue.

As long as the application context is active, the worker is polling the queue. 
It removes the job from the queue, gets a job runner for it from the job runner factory and 
starts the job runner.


