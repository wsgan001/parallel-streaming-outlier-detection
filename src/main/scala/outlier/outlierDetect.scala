package outlier

import java.lang.Iterable
import java.util
import javax.management.Query

import mtree._
import org.apache.flink.api.common.ExecutionMode
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

  //data input
  val data_input: String = "data/stock_100_50.txt"
  //count window variables (total / partitions)
  val count_window: Int = 100000
  val count_slide: Int = 50000
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

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val parallelism = env.getParallelism

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val data = env.readTextFile(data_input)
    val mappedData = data
      .flatMap(line => {
        val splitLine = line.split("&")
        val id = splitLine(0).toInt
        val value = splitLine(1).toDouble
        val multiplication = id / count_slide
        val new_time: Long = cur_time + (multiplication * time_slide)
        var list = new ListBuffer[(Int, Data1d)]
        for (i <- 0 until parallelism) {
          var flag = 0
          if (id % parallelism == i) flag = 0
          else flag = 1
          val tmpEl = (i, new Data1d(value, new_time, flag, id))
          list.+=(tmpEl)
        }
        list
      })

    val timestampData = mappedData
      .assignTimestampsAndWatermarks(new StormTimestamp)

    val keyedData = timestampData
      .keyBy(_._1)
      .timeWindow(Time.milliseconds(time_window), Time.milliseconds(time_slide))
      .allowedLateness(Time.milliseconds(1000))
      .evictor(new StormEvictor)
      .process(new ExactStorm)

    val keyedData2 = keyedData
      .keyBy(_.id % parallelism)
      .timeWindow(Time.milliseconds(time_slide))
      .process(new GroupMetadata)

    val groupedOutliers = keyedData2
      .keyBy(_._1)
      .timeWindow(Time.milliseconds(time_slide))
      .process(new ShowOutliers)

    groupedOutliers.print()

    println("Starting outlier test")
    val timeStart = System.currentTimeMillis()
    env.execute("Outlier-flink")
    val timeEnd = System.currentTimeMillis()
    val time = (timeEnd - timeStart) / 1000

    println("Finished outlier test")
 }

  class StormTimestamp extends AssignerWithPeriodicWatermarks[(Int, Data1d)] with Serializable {

    val maxOutOfOrderness = 1000L // 5 seconds

    override def extractTimestamp(e: (Int, Data1d), prevElementTimestamp: Long) = {
      val timestamp = e._2.arrival
      timestamp
    }

    override def getCurrentWatermark(): Watermark = {
      new Watermark(System.currentTimeMillis - maxOutOfOrderness)
    }
  }

  class StormEvictor extends Evictor[(Int, Data1d), TimeWindow] {
    override def evictBefore(elements: Iterable[TimestampedValue[(Int, Data1d)]], size: Int, window: TimeWindow, evictorContext: Evictor.EvictorContext): Unit = {
      val iteratorEl = elements.iterator
      while (iteratorEl.hasNext) {
        val tmpNode = iteratorEl.next().getValue._2
        if (tmpNode.flag == 1 && tmpNode.arrival >= window.getStart && tmpNode.arrival < window.getEnd - time_slide) iteratorEl.remove()
      }
    }

    override def evictAfter(elements: Iterable[TimestampedValue[(Int, Data1d)]], size: Int, window: TimeWindow, evictorContext: Evictor.EvictorContext): Unit = {
    }
  }

  case class StateTree(var tree: MTree[Data1d])

  class ExactStorm extends ProcessWindowFunction[(Int, Data1d), StormData, Int, TimeWindow] {

    lazy val state: ValueState[StateTree] = getRuntimeContext
      .getState(new ValueStateDescriptor[StateTree]("myTree", classOf[StateTree]))

    override def process(key: Int, context: Context, elements: scala.Iterable[(Int, Data1d)], out: Collector[StormData]): Unit = {
      val time1 = System.currentTimeMillis()
      val window = context.window
      //populate Mtree
      var current: StateTree = state.value
      if (current == null) {
        val nonRandomPromotion = new PromotionFunction[Data1d] {
          /**
            * Chooses (promotes) a pair of objects according to some criteria that is
            * suitable for the application using the M-Tree.
            *
            * @param dataSet          The set of objects to choose a pair from.
            * @param distanceFunction A function that can be used for choosing the
            *                         promoted objects.
            * @return A pair of chosen objects.
            */
          override def process(dataSet: util.Set[Data1d], distanceFunction: DistanceFunction[_ >: Data1d]): utils.Pair[Data1d] = {
            utils.Utils.minMax[Data1d](dataSet)
          }
        }
        val mySplit = new ComposedSplitFunction[Data1d](nonRandomPromotion, new PartitionFunctions.BalancedPartition[Data1d])
        val myTree = new MTree[Data1d](k, count_window + count_slide, DistanceFunctions.EUCLIDEAN, mySplit)
        for (el <- elements) {
          myTree.add(el._2)
        }
        current = StateTree(myTree)
      } else {
        elements
          .filter(el => el._2.arrival >= window.getEnd - time_slide)
          .foreach(el => current.tree.add(el._2))
      }

      //Get neighbors
      elements.foreach(p => {
        val tmpData = new StormData(p._2)
        val query: MTree[Data1d]#Query = current.tree.getNearestByRange(tmpData, range)
        val iter = query.iterator()
        while (iter.hasNext) {
          val node = iter.next().data
          if (node.id != tmpData.id) {
            if (tmpData.arrival >= window.getEnd - time_slide) {
              if (node.flag == 0) {
                if (node.arrival >= window.getEnd - time_slide) {
                  tmpData.count_after += 1
                } else {
                  tmpData.insert_nn_before(node.arrival, k)
                }
              }
            } else {
              if (node.arrival >= window.getEnd - time_slide) {
                tmpData.count_after += 1
              }
            }
          }
        }
          out.collect(tmpData)
      })

      //Remove expiring objects from tree and flagged ones
      elements
        .filter(el => el._2.arrival < window.getStart + time_slide || el._2.flag == 1)
        .foreach(el => current.tree.remove(el._2))
      //update state
      state.update(current)
    }
  }

  case class Metadata(var outliers: Map[Int, StormData])

  class GroupMetadata extends ProcessWindowFunction[StormData, (Long, Int), Int, TimeWindow] {

    lazy val state: ValueState[Metadata] = getRuntimeContext
      .getState(new ValueStateDescriptor[Metadata]("metadata", classOf[Metadata]))

    override def process(key: Int, context: Context, elements: scala.Iterable[StormData], out: Collector[(Long, Int)]): Unit = {
      val time1 = System.currentTimeMillis()
      val window = context.window
      var current: Metadata = state.value
      if (current == null) { //populate list for the first time
        var newMap = Map[Int, StormData]()
        //all elements are new to the window so we have to combine the same ones
        //and add them to the map
        for (el <- elements) {
          val oldEl = newMap.getOrElse(el.id, null)
          if (oldEl == null) {
            newMap += ((el.id, el))
          } else {
            val newValue = combineElements(oldEl, el)
            newMap += ((el.id, newValue))
          }
        }
        //remove safe inliers
        var forRemoval = ListBuffer[Int]()
        for (el <- newMap){
          if(el._2.count_after >= k) forRemoval = forRemoval.+=(el._2.id)
        }
        forRemoval.foreach(el => newMap -= (el))
        current = Metadata(newMap)
      } else { //update list

        //first remove old elements
        var forRemoval = ListBuffer[Int]()
        for (el <- current.outliers.values) {
          if (el.arrival < window.getEnd - time_window) {
            forRemoval = forRemoval.+=(el.id)
          }
        }
        forRemoval.foreach(el => current.outliers -= (el))
        //then insert or combine elements
        for (el <- elements) {
          val oldEl = current.outliers.getOrElse(el.id, null)
          if (oldEl == null && el.arrival >= window.getEnd - time_slide) { //insert new elements
            current.outliers += ((el.id, el))
          } else if(oldEl != null){
            val newValue = combineElements(oldEl, el)
            current.outliers += ((el.id, newValue))
          }
        }
        //remove safe inliers
        forRemoval = ListBuffer[Int]()
        for (el <- current.outliers.values){
          if(el.count_after >= k) forRemoval = forRemoval.+=(el.id)
        }
        forRemoval.foreach(el => current.outliers -= (el))
      }
      state.update(current)

      var outliers = ListBuffer[Int]()
      for (el <- current.outliers.values) {
        val nnBefore = el.nn_before.count(_ >= window.getEnd - time_window)
        if (nnBefore + el.count_after < k) outliers.+=(el.id)
      }

      out.collect((window.getEnd, outliers.size))
    }

    def combineElements(el1: StormData, el2: StormData): StormData = {
      el1.count_after += el2.count_after
      for (elem <- el2.nn_before) {
        el1.insert_nn_before(elem, k)
      }
      el1
    }

  }

  class ShowOutliers extends ProcessWindowFunction[(Long, Int), String, Long, TimeWindow] {

    override def process(key: Long, context: Context, elements: scala.Iterable[(Long, Int)], out: Collector[String]): Unit = {
      val outliers = elements.toList.map(_._2).sum
      out.collect(s"$key;$outliers")
    }
  }

}
