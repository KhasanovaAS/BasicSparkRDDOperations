import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object TestScala1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()

    conf.setAppName("Spark Hello World")
    conf.setMaster("local[2]")

    val sc = new SparkContext(conf)

    val file = sc.textFile("products.csv")//RDD

    val wordRDD=file.flatMap(line=>line.split(" "))

    val wordRDD1=wordRDD.map(line=>(line,1)).reduceByKey((a,b)=>a+b)

    wordRDD1.foreach(println)
  }
}
