package examples.rdd.count
import org.apache.spark.{SparkConf, SparkContext}

object CountWikiSpark {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("wikiSparkCount").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile("in/wikiSpark.text") //: RDD[String]
    val words = textFile.flatMap(line => line.split(" "))
    val wordCounts = words.countByValue()

    println("CountByValue:")
    for ((word, count) <- wordCounts) println(word + " : " + count)
    println("Count: " + words.count )
    println("Unique words Count: " + wordCounts.size )
  }
}
