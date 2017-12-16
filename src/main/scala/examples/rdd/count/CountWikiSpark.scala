package examples.rdd.count
import org.apache.spark.{SparkConf, SparkContext}

object CountWikiSpark {
  def main(args: Array[String]) {
    val masterSet = "local[*]"
    val conf = new SparkConf().setAppName("wikiSparkCount").setMaster(masterSet)
    val sc = new SparkContext(conf)

    val textFile = sc.textFile("in/wikiSpark.text") //: RDD[String]
    val words = textFile.flatMap(line => line.split(" ")).filter(!_.isEmpty)
    val wordCountsRDD = words.map(word => (word.toLowerCase, 1)).reduceByKey(_ + _)
    val wordCounts = wordCountsRDD.collect()

    println("CountByValue:")
    for ((word, count) <- wordCounts) println(word + " : " + count)
    println("Count: " + words.count )
    println("Unique words Count: " + wordCounts.size )

    wordCountsRDD.saveAsTextFile("out/count/wikiSparkCount-" + masterSet + "-" + System.currentTimeMillis)
  }
}
