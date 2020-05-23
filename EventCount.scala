package ie.ucd.csl.comp47470

import java.io.File
import java.io.PrintWriter
import java.util.concurrent.atomic.AtomicInteger

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{ManualClockWrapper, Minutes, StreamingContext, Time, StateSpec, State}
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}
import org.apache.spark.util.LongAccumulator
import org.rogach.scallop._

import scala.collection.mutable

import geotrellis.vector.{Point, Polygon}
import geotrellis.proj4._


class EventCountConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, checkpoint, output)
  val input = opt[String](descr = "input path", required = true)
  val checkpoint = opt[String](descr = "checkpoint path", required = true)
  val output = opt[String](descr = "output path", required = true)
  verify()
}

object EventCount {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]): Unit = {
    val args = new EventCountConf(argv)

    log.info("Input: " + args.input())

    val spark = SparkSession
      .builder()
      .config("spark.streaming.clock", "org.apache.spark.util.ManualClock")
      .appName("EventCount")
      .getOrCreate()

    val numCompletedRDDs = spark.sparkContext.longAccumulator("number of completed RDDs")

    val batchDuration = Minutes(1)
    val ssc = new StreamingContext(spark.sparkContext, batchDuration)
    val batchListener = new StreamingContextBatchCompletionListener(ssc, 24)
    ssc.addStreamingListener(batchListener)

    val rdds = buildMockStream(ssc.sparkContext, args.input())
    val inputData: mutable.Queue[RDD[String]] = mutable.Queue()
    val stream = ssc.queueStream(inputData)

    val wc = stream.map(_.split(","))
      .map(tuple => ("all", 1))
      .reduceByKeyAndWindow(
        (x: Int, y: Int) => x + y, (x: Int, y: Int) => x - y, Minutes(60), Minutes(60))
      .persist()

    wc.saveAsTextFiles(args.output())

    wc.foreachRDD(rdd => {
      numCompletedRDDs.add(1L)
    })
    ssc.checkpoint(args.checkpoint())
    ssc.start()

    for (rdd <- rdds) {
      inputData += rdd
      ManualClockWrapper.advanceManualClock(ssc, batchDuration.milliseconds, 50L)
    }

    batchListener.waitUntilCompleted(() =>
      ssc.stop()
    )
  }

  class StreamingContextBatchCompletionListener(val ssc: StreamingContext, val limit: Int) extends StreamingListener {
    def waitUntilCompleted(cleanUpFunc: () => Unit): Unit = {
      while (!sparkExSeen) {}
      cleanUpFunc()
    }

    val numBatchesExecuted = new AtomicInteger(0)
    @volatile var sparkExSeen = false

    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
      val curNumBatches = numBatchesExecuted.incrementAndGet()
      log.info(s"${curNumBatches} batches have been executed")
      if (curNumBatches == limit) {
        sparkExSeen = true
      }
    }
  }

  def buildMockStream(sc: SparkContext, directoryName: String): Array[RDD[String]] = {
    val d = new File(directoryName)
    if (d.exists() && d.isDirectory) {
      d.listFiles
        .filter(file => file.isFile && file.getName.startsWith("part-"))
        .map(file => d.getAbsolutePath + "/" + file.getName).sorted
        .map(path => sc.textFile(path))
    } else {
      throw new IllegalArgumentException(s"$directoryName is not a valid directory containing part files!")
    }
  }
}

object RegionEventCount {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]): Unit = {
    val args = new EventCountConf(argv)

    log.info("Input: " + args.input())

    val spark = SparkSession
      .builder()
      .config("spark.streaming.clock", "org.apache.spark.util.ManualClock")
      .appName("EventCount")
      .getOrCreate()

    val numCompletedRDDs = spark.sparkContext.longAccumulator("number of completed RDDs")

    val batchDuration = Minutes(1)
    val ssc = new StreamingContext(spark.sparkContext, batchDuration)
    val batchListener = new StreamingContextBatchCompletionListener(ssc, 24)
    ssc.addStreamingListener(batchListener)

    val rdds = buildMockStream(ssc.sparkContext, args.input())
    val inputData: mutable.Queue[RDD[String]] = mutable.Queue()
    val stream = ssc.queueStream(inputData)

    val latLongToWebMercator = Transform(LatLng, WebMercator)
    //goldman
    val p1 : Polygon = Polygon(latLongToWebMercator(-74.0141012, 40.7152191), latLongToWebMercator(-74.013777, 40.7152275), latLongToWebMercator(-74.0141027, 40.7138745), latLongToWebMercator(-74.0144185, 40.7140753), latLongToWebMercator(-74.0141012, 40.7152191))
    //citygroup
    val p2 : Polygon = Polygon(latLongToWebMercator(-74.011869, 40.7217236), latLongToWebMercator(-74.009867, 40.721493), latLongToWebMercator(-74.010140,40.720053), latLongToWebMercator(-74.012083, 40.720267), latLongToWebMercator(-74.011869, 40.7217236))
    //Code for q2 here (uncomment the following lines):
    val wc = stream.map(_.split(","))
      .map(tuple => {
        if(tuple(0) == "green"){
          val myPoint : Point = Point(latLongToWebMercator(tuple(8).toDouble, tuple(9).toDouble))
          if(myPoint.within(p1)){
            ("goldman",1)
          }else if(myPoint.within(p2)){
            ("citigroup",1)
          }else{
            ("else",1)
          }
        }else{
          val myPoint : Point = Point(latLongToWebMercator(tuple(10).toDouble, tuple(11).toDouble))
          if(myPoint.within(p1)){
            ("goldman",1)
          }else if(myPoint.within(p2)){
            ("citigroup",1)
          }else{
            ("else",1)
          }
        }
      })
      .filter(p => (p._1 != "else"))
      .reduceByKeyAndWindow(
        (x: Int, y: Int) => x + y, (x: Int, y: Int) => x - y, Minutes(60), Minutes(60))
      .persist()

    wc.saveAsTextFiles(args.output())

    wc.foreachRDD(rdd => {
      numCompletedRDDs.add(1L)
    })
    ssc.checkpoint(args.checkpoint())
    ssc.start()

    for (rdd <- rdds) {
      inputData += rdd
      ManualClockWrapper.advanceManualClock(ssc, batchDuration.milliseconds, 50L)
    }

    batchListener.waitUntilCompleted(() =>
      ssc.stop()
    )
  }

  class StreamingContextBatchCompletionListener(val ssc: StreamingContext, val limit: Int) extends StreamingListener {
    def waitUntilCompleted(cleanUpFunc: () => Unit): Unit = {
      while (!sparkExSeen) {}
      cleanUpFunc()
    }

    val numBatchesExecuted = new AtomicInteger(0)
    @volatile var sparkExSeen = false

    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
      val curNumBatches = numBatchesExecuted.incrementAndGet()
      log.info(s"${curNumBatches} batches have been executed")
      if (curNumBatches == limit) {
        sparkExSeen = true
      }
    }
  }

  def buildMockStream(sc: SparkContext, directoryName: String): Array[RDD[String]] = {
    val d = new File(directoryName)
    if (d.exists() && d.isDirectory) {
      d.listFiles
        .filter(file => file.isFile && file.getName.startsWith("part-"))
        .map(file => d.getAbsolutePath + "/" + file.getName).sorted
        .map(path => sc.textFile(path))
    } else {
      throw new IllegalArgumentException(s"$directoryName is not a valid directory containing part files!")
    }
  }
}

// arrival configuration class (for Q3)
class ArrivalConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, checkpoint, output)
  val input = opt[String](descr = "input path", required = true)
  val checkpoint = opt[String](descr = "checkpoint path", required = true)
  val output = opt[String](descr = "output path", required = true)
  verify()
}

// for Q3
case class temptupleClass(current: Int, time_stamp: String, previous: Int) extends Serializable


object TrendingArrivals {
  val log = Logger.getLogger(getClass().getName())

  //function for trend arrivals
  def trackStateForTrendArrival(batchTime: Time, key: String, value: Option[Int], state: State[temptupleClass]) : Option[(String, temptupleClass)] = {
    var previous = 0
    if (state.exists()) {
      previous = state.get().current
    }
    var current = value.getOrElse(0).toInt
    var batchTime_ = batchTime.milliseconds
    if ((current >= 10) && (current >= 2*previous)){
      if (key == "goldman")
        println(s"Number of arrivals to Goldman Sachs has doubled from $previous to $current at $batchTime_!")
      else
        println(s"Number of arrivals to Citigroup has doubled from $previous to $current at $batchTime_!")
    }
    var temp = temptupleClass(current = current, time_stamp = "%08d".format(batchTime_), previous = previous)
    state.update(temp)
    Some((key,temp))
  }

  def main(argv: Array[String]): Unit = {
    val args = new ArrivalConf(argv)

    log.info("Input: " + args.input())

    val spark = SparkSession
      .builder()
      .config("spark.streaming.clock", "org.apache.spark.util.ManualClock")
      .appName("EventCount")
      .getOrCreate()

    val numCompletedRDDs = spark.sparkContext.longAccumulator("number of completed RDDs")

    val batchDuration = Minutes(1)
    val ssc = new StreamingContext(spark.sparkContext, batchDuration)
    val batchListener = new StreamingContextBatchCompletionListener(ssc, 24)
    ssc.addStreamingListener(batchListener)

    val rdds = buildMockStream(ssc.sparkContext, args.input())
    val inputData: mutable.Queue[RDD[String]] = mutable.Queue()
    val stream = ssc.queueStream(inputData)

    val latLongToWebMercator = Transform(LatLng, WebMercator)
    val p1 : Polygon = Polygon(latLongToWebMercator(-74.0141012, 40.7152191), latLongToWebMercator(-74.013777, 40.7152275), latLongToWebMercator(-74.0141027, 40.7138745), latLongToWebMercator(-74.0144185, 40.7140753), latLongToWebMercator(-74.0141012, 40.7152191))
    val p2 : Polygon = Polygon(latLongToWebMercator(-74.011869, 40.7217236), latLongToWebMercator(-74.009867, 40.721493), latLongToWebMercator(-74.010140,40.720053), latLongToWebMercator(-74.012083, 40.720267), latLongToWebMercator(-74.011869, 40.7217236))

    // Code for q3, uncomment the following lines
    val stateSpec = StateSpec.function(trackStateForTrendArrival _)
    val wc = stream.map(_.split(","))
      .map(tuple => {
        if(tuple(0) == "green"){
          val myPoint : Point = Point(latLongToWebMercator(tuple(8).toDouble, tuple(9).toDouble))
          if(myPoint.within(p1)){
            ("goldman",1)
          }else if(myPoint.within(p2)){
            ("citigroup",1)
          }else{
            ("other",1)
          }
        }else{
          val myPoint : Point = Point(latLongToWebMercator(tuple(10).toDouble, tuple(11).toDouble))
          if(myPoint.within(p1)){
            ("goldman",1)
          }else if(myPoint.within(p2)){
            ("citigroup",1)
          }else{
            ("other",1)
          }
        }
      })
      .filter(p => (p._1 != "other"))
      .reduceByKeyAndWindow(
        (x: Int, y: Int) => x + y, (x: Int, y: Int) => x - y, Minutes(10), Minutes(10))
      .mapWithState(stateSpec)

//    wc.saveAsTextFiles(args.output()+"/part")
//
//    wc.foreachRDD(rdd => {
//      numCompletedRDDs.add(1L)
//    })
//
//    //wc.foreachRDD((rdd, time)  => {
//    //  numCompletedRDDs.add(1L)
//    //})

    var output_d = args.output()
    val snapRdd = wc.stateSnapshots()

    snapRdd.foreachRDD( (rdd, time) => {
      var updatedRDD = rdd.map(line => (line._1,(line._2.current,line._2.time_stamp,line._2.previous)))
      updatedRDD.saveAsTextFile(output_d+"/part-"+"%08d".format(time.milliseconds))
    })

    snapRdd.foreachRDD(rdd => {
      numCompletedRDDs.add(1L)
    })
    
    ssc.checkpoint(args.checkpoint())
    ssc.start()

    for (rdd <- rdds) {
      inputData += rdd
      ManualClockWrapper.advanceManualClock(ssc, batchDuration.milliseconds, 50L)
    }

    batchListener.waitUntilCompleted(() =>
      ssc.stop()
    )
  }

  class StreamingContextBatchCompletionListener(val ssc: StreamingContext, val limit: Int) extends StreamingListener {
    def waitUntilCompleted(cleanUpFunc: () => Unit): Unit = {
      while (!sparkExSeen) {}
      cleanUpFunc()
    }

    val numBatchesExecuted = new AtomicInteger(0)
    @volatile var sparkExSeen = false

    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
      val curNumBatches = numBatchesExecuted.incrementAndGet()
      log.info(s"${curNumBatches} batches have been executed")
      if (curNumBatches == limit) {
        sparkExSeen = true
      }
    }
  }

  def buildMockStream(sc: SparkContext, directoryName: String): Array[RDD[String]] = {
    val d = new File(directoryName)
    if (d.exists() && d.isDirectory) {
      d.listFiles
        .filter(file => file.isFile && file.getName.startsWith("part-"))
        .map(file => d.getAbsolutePath + "/" + file.getName).sorted
        .map(path => sc.textFile(path))
    } else {
      throw new IllegalArgumentException(s"$directoryName is not a valid directory containing part files!")
    }
  }
}
