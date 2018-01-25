package outlier

import java.lang.Iterable
import java.util
import javax.management.Query

import mtree._
import org.apache.flink.api.common.functions.{FlatMapFunction, Partitioner}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, ProcessFunction}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala.function.{AllWindowFunction, ProcessAllWindowFunction, ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.evictors.Evictor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer
import scala.util.Random
import scala.util.control.Breaks._

object outlierDetect {

  //partitioning
  val parallelism: Int = 10
  //count window variables (total / partitions)
  val count_window: Int = 10000
  val count_slide: Int = 500
  val count_slide_percent: Double = 100 * (count_slide.toDouble / count_window)
  //time window variables
  val time_window: Int = count_window / 10
  val time_slide: Int = (time_window * (count_slide_percent / 100)).toInt
  //distance outlier variables
  val k: Int = 50
  val range: Double = 0.45
  //source variables
  val randomGenerate: Int = 100
  val stopStreamAt: Int = 2000
  //stats
  var times_per_slide = Map[String, Long]()
  //helper to slow down stream
  val cur_time = System.currentTimeMillis() + 1000000L //some delay for the correct timestamp

  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.createLocalEnvironment(parallelism)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    val data = env.readTextFile("DummyData/stock/stock_id_20k.txt")
    val mappedData = data
      .flatMap(line => {
        val splitLine = line.split("&")
        val id = splitLine(0).toInt
        val value = splitLine(1).toDouble
        val multiplication = id / count_slide
        val new_time: Long = cur_time + (multiplication * time_slide)
        var list = new ListBuffer[(Int, StormData)]
        for (i <- 0 until parallelism) {
          var flag = 0
          if (id % parallelism == i) flag = 0
          else flag = 1
          val tmpEl = (i, new StormData(new Data1d(value, new_time, flag, id)))
          list.+=(tmpEl)
        }
        list
      })

    val timestampData = mappedData
      .assignTimestampsAndWatermarks(new StormTimestamp)

    val keyedData = timestampData
      .keyBy(_._1)
      .timeWindow(Time.milliseconds(time_window), Time.milliseconds(time_slide))
      .allowedLateness(Time.milliseconds(5000))
      .evictor(new StormEvictor)
      .process(new ExactStorm)

    val keyedData2 = keyedData
      .keyBy(_.id % parallelism)
      .timeWindow(Time.milliseconds(time_slide))
      .process(new GroupMetadata)
////      .reduce((a,b) => a)

    keyedData2.print()

//    val groupedOutliers = keyedData2
//      .keyBy(_._1)
//      .timeWindow(Time.milliseconds(time_slide))
//      .process(new ShowOutliers)

//    groupedOutliers.print()

    println("Starting outlier test")
    val timeStart = System.currentTimeMillis()
    env.execute("Outlier-flink")
    val timeEnd = System.currentTimeMillis()
    val time = (timeEnd - timeStart) / 1000

    println("Finished outlier test")
    println("Total run time: " + time + " sec")
    val total_slides = times_per_slide.size
    println(s"Total Slides: $total_slides")
    println(s"Average time per slide: ${times_per_slide.values.sum.toDouble / total_slides / 1000}")
  }

  class StormTimestamp extends AssignerWithPeriodicWatermarks[(Int, StormData)] with Serializable {

    val maxOutOfOrderness = 5000L // 5 seconds

    override def extractTimestamp(e: (Int, StormData), prevElementTimestamp: Long) = {
      val timestamp = e._2.arrival
      timestamp
    }

    override def getCurrentWatermark(): Watermark = {
      new Watermark(System.currentTimeMillis - maxOutOfOrderness)
    }
  }

  class StormEvictor extends Evictor[(Int, StormData), TimeWindow] {
    override def evictBefore(elements: Iterable[TimestampedValue[(Int, StormData)]], size: Int, window: TimeWindow, evictorContext: Evictor.EvictorContext): Unit = {
      val iteratorEl = elements.iterator
      while (iteratorEl.hasNext) {
        val tmpNode = iteratorEl.next().getValue._2
        if (tmpNode.flag == 1 && tmpNode.arrival >= window.getStart && tmpNode.arrival < window.getEnd - time_slide) iteratorEl.remove()
      }
    }

    override def evictAfter(elements: Iterable[TimestampedValue[(Int, StormData)]], size: Int, window: TimeWindow, evictorContext: Evictor.EvictorContext): Unit = {
    }
  }

  class ExactStorm extends ProcessWindowFunction[(Int, StormData), StormData, Int, TimeWindow] {

    override def process(key: Int, context: Context, elements: scala.Iterable[(Int, StormData)], out: Collector[StormData]): Unit = {
      val time1 = System.currentTimeMillis()
      if (elements.size > k) {
        val window = context.window
        val inputList = elements.map(_._2).toList

        inputList.filter(_.arrival >= window.getEnd - time_slide).foreach(p => {
          refreshList(p, inputList, window)
        })
        inputList.foreach(p => out.collect(p))

        //update stats
        val time2 = System.currentTimeMillis()
        val tmpkey = window.getEnd.toString
        val tmpvalue = time2 - time1
        val oldValue = times_per_slide.getOrElse(tmpkey, null)
        if (oldValue == null) {
          times_per_slide += ((tmpkey, tmpvalue))
        } else {
          val tmpValue = oldValue.toString.toLong
          val newValue = tmpValue + tmpvalue
          times_per_slide += ((tmpkey, newValue))
        }
      }
    }

    def refreshList(node: StormData, nodes: List[StormData], window: TimeWindow): Unit = {
      if (nodes.size != 0) {
        val neighbors = nodes
          .map(x => (x, distance(x,node)))
          .filter(_._2 <= range).map(_._1)

        neighbors
          .filter(_.arrival < window.getEnd - time_slide)
          .foreach(p => node.nn_before.+=(p.arrival)) //add previous neighbors to new node

        neighbors
          .filter(p => p.arrival >= window.getEnd - time_slide && p.id != node.id)
          .foreach(p => node.count_after += 1)

        nodes
          .filter(x => x.arrival < window.getEnd - time_slide)
          .filter(x => neighbors.contains(x))
          .foreach(n => n.count_after += 1) //add new neighbor to previous nodes
      }
    }

    def distance(xs: StormData, ys: StormData): Double = {
      val value = scala.math.pow(xs.value - ys.value, 2)
      val res = scala.math.sqrt(value)
      res
    }


  }

  case class Metadata(var outliers: Map[Int, StormData])

  class GroupMetadata extends ProcessWindowFunction[StormData, String, Int, TimeWindow] {

    override def process(key: Int, context: Context, elements: scala.Iterable[StormData], out: Collector[String]): Unit = {

      out.collect("test")

    }
  }

  class ShowOutliers extends ProcessWindowFunction[(Long,Int), String, Long, TimeWindow] {

    override def process(key: Long, context: Context, elements: scala.Iterable[(Long,Int)], out: Collector[String]): Unit = {
      val outliers = elements.toList.map(_._2).sum
      out.collect(s"window: $key outliers: $outliers")
    }
  }

  class ExactStormDebug extends ProcessWindowFunction[(Int, StormData), String, Int, TimeWindow] {

    override def process(key: Int, context: Context, elements: scala.Iterable[(Int, StormData)], out: Collector[String]): Unit = {
      val time1 = System.currentTimeMillis()
      if (elements.size > k) {
        val window = context.window
        val inputList = elements.map(_._2).toList

        inputList.filter(_.arrival >= window.getEnd - time_slide).foreach(p => {
          refreshList(p, inputList, window)
        })
        inputList.foreach(p => System.currentTimeMillis())

        //update stats
        val time2 = System.currentTimeMillis()
        val tmpkey = window.getEnd.toString
        val tmpvalue = time2 - time1
        val oldValue = times_per_slide.getOrElse(tmpkey, null)
        if (oldValue == null) {
          times_per_slide += ((tmpkey, tmpvalue))
        } else {
          val tmpValue = oldValue.toString.toLong
          val newValue = tmpValue + tmpvalue
          times_per_slide += ((tmpkey, newValue))
        }
        out.collect(s"${elements.size}")
      }
    }

    def refreshList(node: StormData, nodes: List[StormData], window: TimeWindow): Unit = {
      if (nodes.size != 0) {
        val neighbors = nodes
          .map(x => (x, distance(x,node)))
          .filter(_._2 <= range).map(_._1)

        neighbors
          .filter(_.arrival < window.getEnd - time_slide)
          .foreach(p => node.nn_before.+=(p.arrival)) //add previous neighbors to new node

        neighbors
          .filter(p => p.arrival >= window.getEnd - time_slide && p.id != node.id)
          .foreach(p => node.count_after += 1)

        nodes
          .filter(x => x.arrival < window.getEnd - time_slide)
          .filter(x => neighbors.contains(x))
          .foreach(n => n.count_after += 1) //add new neighbor to previous nodes
      }
    }

    def distance(xs: StormData, ys: StormData): Double = {
      val value = scala.math.pow(xs.value - ys.value, 2)
      val res = scala.math.sqrt(value)
      res
    }


  }


}
