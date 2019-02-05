package com.github.megachucky.kafka.streams.machinelearning


import java.util.Properties
import java.util.concurrent.TimeUnit

import hex.genmodel.GenModel
import hex.genmodel.easy.prediction.BinomialModelPrediction
import hex.genmodel.easy.{EasyPredictModelWrapper, RowData}
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

class Kafka_Streams_MachineLearning_H2O_GBM_SCALA_Example extends App{

  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.Serdes._
  // Name of the generated H2O model
  private val modelClassName = "com.github.megachucky.kafka.streams.machinelearning.models.gbm_pojo_test"

  // Prediction Value
  private var airlineDelayPreduction = "unknown"



    var rawModel = Class.forName(modelClassName).newInstance.asInstanceOf[GenModel]
    val model = new EasyPredictModelWrapper(rawModel)
    // Configure Kafka Streams Application
    val bootstrapServers = if (args.length > 0) args(0) else "localhost:9092"


  val streamsConfiguration: Properties = {

    val p = new Properties()
    // Give the Streams application a unique name. The name must be unique
    // in the Kafka cluster
    // against which the application is run.
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-h2o-gbm-example")
    // Where to find Kafka broker(s).
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    // Specify default (de)serializers for record keys and for record
    // values.
    p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
    p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
    // For illustrative purposes we disable record caches

    p.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, new Integer(0));
    p
  }

  def toRow : String =>Iterable[RowData]= { value =>
    // Year,Month,DayofMonth,DayOfWeek,DepTime,CRSDepTime,ArrTime,CRSArrTime,UniqueCarrier,FlightNum,TailNum,ActualElapsedTime,CRSElapsedTime,AirTime,ArrDelay,DepDelay,Origin,Dest,Distance,TaxiIn,TaxiOut,Cancelled,CancellationCode,Diverted,CarrierDelay,WeatherDelay,NASDelay,SecurityDelay,LateAircraftDelay,IsArrDelayed,IsDepDelayed
    // value:
    // YES, probably delayed:
    // 1987,10,14,3,741,730,912,849,PS,1451,NA,91,79,NA,23,11,SAN,SFO,447,NA,NA,0,NA,0,NA,NA,NA,NA,NA,YES,YES
    // NO, probably not delayed:
    // 1999,10,14,3,741,730,912,849,PS,1451,NA,91,79,NA,23,11,SAN,SFO,447,NA,NA,0,NA,0,NA,NA,NA,NA,NA,YES,YES
    if (value != null && !(value == "")) {
      println("#####################")
      println("Flight Input:" + value)
      val valuesArray = value.split(",")
      val row = new RowData
      row.put("Year", valuesArray(0))
      row.put("Month", valuesArray(1))
      row.put("DayofMonth", valuesArray(2))
      row.put("DayOfWeek", valuesArray(3))
      row.put("CRSDepTime", valuesArray(5))
      row.put("UniqueCarrier", valuesArray(8))
      row.put("Origin", valuesArray(16))
      row.put("Dest", valuesArray(17))
      Some(row)
    }
    else
      None

  }

  def echoPrediction(p: BinomialModelPrediction): Unit ={
    println("Label (aka prediction) is flight departure delayed: " + p.label)
    print("Class probabilities: ")
    var i = 0
    for(i<- 0 to p.classProbabilities.length)
    {
      if (i > 0) print(",")
      print(p.classProbabilities(i))

    }
    println("")
    println("#####################")
  }

    // In the subsequent lines we define the processing topology of the
    // Streams application.
    val builder = new StreamsBuilder
    // Construct a `KStream` from the input topic "AirlineInputTopic", where
    // message values
    // represent lines of text (for the sake of this example, we ignore
    // whatever may be stored
    // in the message keys).
    val airlineInputLines :  KStream[String, String] = builder.stream[String,String]("AirlineInputTopic")
    // Stream Processor (in this case 'foreach' to add custom logic, i.e. apply the
    // analytic model)

    airlineInputLines.flatMapValues(toRow).mapValues( row =>{


        val p = model.predictBinomial(row)

        airlineDelayPreduction = p.label
        echoPrediction(p)

      })



    // airlineInputLines.print();
    // Transform message: Add prediction information
    val transformedMessage = airlineInputLines.mapValues( _ => ("Prediction: Is Airline delayed? =>" + airlineDelayPreduction))
    // Send prediction information to Output Topic
    transformedMessage.to("AirlineOutputTopic")
    // Start Kafka Streams Application to process new incoming messages from Input
    // Topic
    val streams = new KafkaStreams(builder.build, streamsConfiguration)
    streams.cleanUp()
    streams.start()
    println("Airline Delay Prediction Microservice is running...")
    println("Input to Kafka Topic 'AirlineInputTopic'; Output to Kafka Topic 'AirlineOutputTopic'")
    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka
    // Streams

    sys.ShutdownHookThread {
      streams.close(10, TimeUnit.SECONDS)
    }

}
