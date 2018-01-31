package outlier

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object outlierDetect {

  //data input
  var data_input = "DummyData/stock/stock_id_20k.txt"
  //partitioning
  var parallelism: Int = 1
  //count window variables (total / partitions)
  var count_window: Int = 10000
  var count_slide: Int = 500
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

    if (args.length != 4) {
      println("Wrong arguments!")
      System.exit(1)
    }

    //parallelism = args(0).toInt
    count_window = args(1).toInt
    count_slide = args(2).toInt
    data_input = args(3)

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(parallelism)

    //val env = StreamExecutionEnvironment.createLocalEnvironment(parallelism)
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

    val resultData = timestampData
      .timeWindowAll(Time.milliseconds(time_window), Time.milliseconds(time_slide))
      .allowedLateness(Time.milliseconds(5000))
      .apply(new StormBasic)

    resultData.print()

    println("Starting outlier test")
    val timeStart = System.currentTimeMillis()
    env.execute("Outlier-flink")
    val timeEnd = System.currentTimeMillis()
    val time = (timeEnd - timeStart) / 1000

    println("Finished outlier test")
    //    println("Total run time: " + time + " sec")
    //    val total_slides = times_per_slide.size
    //    println(s"Total Slides: $total_slides")
    //    println(s"Average time per slide: ${times_per_slide.values.sum.toDouble / total_slides / 1000}")
  }

  class StormTimestamp extends AssignerWithPeriodicWatermarks[(Int, StormData)] with Serializable {

    val maxOutOfOrderness = 5000L // 30 seconds

    override def extractTimestamp(e: (Int, StormData), prevElementTimestamp: Long) = {
      val timestamp = e._2.arrival
      timestamp
    }

    override def getCurrentWatermark(): Watermark = {
      new Watermark(System.currentTimeMillis - maxOutOfOrderness)
    }
  }

  class StormBasic extends AllWindowFunction[(Int, StormData), String, TimeWindow] {
    override def apply(window: TimeWindow, input: scala.Iterable[(Int, StormData)], out: Collector[String]): Unit = {
      //      val time1 = System.currentTimeMillis()
      val inputList = input.map(line => line._2).toList
      inputList.filter(_.arrival >= window.getEnd - time_slide).foreach(p => {
        refreshList(p, inputList, window)
      })
      val outliers = inputList
        .filter(n => {
          val value1 = n.count_after
          val value2 = n.nn_before.count(p => p >= window.getStart)
          value1 + value2 < k
        })
      out.collect(s"${window.getEnd};${outliers.size}")
      //      val time2 = System.currentTimeMillis()
      //      times_per_slide += ((window.getEnd.toString, time2 - time1))
    }

    def refreshList(node: StormData, nodes: List[StormData], window: TimeWindow): Unit = {
      if (nodes.nonEmpty) {
        val neighbors = nodes
          .filter(_.id != node.id)
          .map(x => (x, distance(x, node)))
          .filter(_._2 <= range).map(_._1)

        neighbors
          .foreach(x => {
            if (x.arrival < window.getEnd - time_slide)
              node.insert_nn_before(x.arrival, k)
            else
              node.count_after += 1
          })

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
