package cse6331.capstone

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object Crimes {
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("Crimes").setMaster("local[2]")
		val sc = new SparkContext(conf)
		var i = 0
		
		var input = sc.textFile(args(0)).map(line=>{
		  var x = line.split(",") 
		  if(x.length == 4) {
		    (x(0).toString(),x(1).toString(),x(2).toInt,x(3).toInt)
		  } else {
		    (null,null,null,null)
		  }
		})
		
		input = input.filter(x => x._1 != null)
		
		
		println("Count: " + input.count())
		sc.stop()

	}
}