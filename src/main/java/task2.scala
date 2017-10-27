import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object TestScala2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()

    conf.setAppName("Spark Hello World")
    conf.setMaster("local[2]")

    val sc = new SparkContext(conf)


    ///1

    val file = sc.textFile("file.txt")//RDD[string]
    val row=file.map(s=>s.split(" "))
    val numbers = row.map(s=>s.map(t=>t.toInt))
    numbers.map(x=>x.mkString(" ")).foreach(println)

    val rS=numbers.map(x=>x.reduce((a,b)=>a+b))
    println("Sum in rows = ")
    rS.foreach(println)

    val SM5=numbers.map(x=>x.filter(a=>(a%5==0)).reduce((b,c)=>b+c))
    println("Sum of numbers which are multiples of 5 = ")
    SM5.foreach(println)

    val Mx=numbers.map(x=>x.max)
    println("Max in each row = ")
    Mx.foreach(println)

    val Mn=numbers.map(x=>x.min)
    println("Min in each row = ")
    Mn.foreach(println)

    val Dt=numbers.map(x=>x.distinct)
    println("Set of distinct in each row = ")
    Dt.map(x=>x.mkString(" ")).foreach(println)


    ///2

    val RDDword=file.flatMap(line=>line.split(' ')).map(x=>x.toInt)
    val S=RDDword.reduce((x,y)=>x+y)
    println("Sum = ",S)
    //RDDword.foreach(println)
    val MultOf5=RDDword.filter(x=>(x%5==0)).reduce((x, y)=>x+y)
    println("Multiples of 5 = ",MultOf5)

    val Max=RDDword.max()
    println("Max = ",Max)

    val Min=RDDword.min()
    println("Min = ",Min)

    val Dist=RDDword.distinct()
    Dist.foreach(println)

  }
}
