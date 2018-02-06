package outlier

import java.lang.Iterable

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.evictors.Evictor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object outlierDetect {

  //data input
  var data_input = "data/stock_100_50.txt"
  //count window variables (total / partitions)
  var count_window: Int = 100000
  var count_slide: Int = 50000
  var count_slide_percent: Double = 100 * (count_slide.toDouble / count_window)
  //time window variables
  var time_window: Int = count_window / 10
  var time_slide: Int = (time_window * (count_slide_percent / 100)).toInt
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

  class StormTimestamp extends AssignerWithPeriodicWatermarks[(Int, StormData)] with Serializable {

    val maxOutOfOrderness = 1000L // 1 seconds

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
      val window = context.window
      val inputList = elements.map(_._2).toList

      inputList.filter(_.arrival >= window.getEnd - time_slide).foreach(p => {
        refreshList(p, inputList, context.window)
      })
      inputList.foreach(p => {
        if (!p.safe_inlier) {
          out.collect(p)
        }
      })
    }

    def refreshList(node: StormData, nodes: List[StormData], window: TimeWindow): Unit = {
      if (nodes.nonEmpty) {
        val neighbors = nodes
          .filter(_.id != node.id)
          .map(x => (x, distance(x, node)))
          .filter(_._2 <= range).map(_._1)

        neighbors
          .foreach(x => {
            if (x.arrival < window.getEnd - time_slide) {
              node.insert_nn_before(x.arrival, k)
            } else {
              node.count_after += 1
              if (node.count_after >= k) {
                node.safe_inlier = true
              }
            }
          })

        nodes
          .filter(x => x.arrival < window.getEnd - time_slide && neighbors.contains(x))
          .foreach(n => {
            n.count_after += 1
            if (n.count_after >= k) {
              n.safe_inlier = true
            }
          }) //add new neighbor to previous nodes
      }
    }

    def distance(xs: StormData, ys: StormData): Double = {
      val value = scala.math.pow(xs.value - ys.value, 2)
      val res = scala.math.sqrt(value)
      res
    }

  }

  case class Metadata(var outliers: Map[Int, StormData])

  class GroupMetadata extends ProcessWindowFunction[(StormData), (Long, Int), Int, TimeWindow] {

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
        current = Metadata(newMap)
      } else { //update list

        //first remove old elements and elements that are safe inliers
        var forRemoval = ListBuffer[Int]()
        for (el <- current.outliers.values) {
          if (elements.count(_.id == el.id) == 0) {
            forRemoval = forRemoval.+=(el.id)
          }
        }
        forRemoval.foreach(el => current.outliers -= (el))
        //then insert or combine elements
        for (el <- elements) {
          val oldEl = current.outliers.getOrElse(el.id, null)
          if (oldEl == null) {
            current.outliers += ((el.id, el))
          } else {
            if (el.arrival < window.getEnd - time_slide) {
              oldEl.count_after = el.count_after
              current.outliers += ((el.id, oldEl))
            } else {
              val newValue = combineElements(oldEl, el)
              current.outliers += ((el.id, newValue))
            }
          }
        }
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
