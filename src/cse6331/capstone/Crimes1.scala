package cse6331.capstone

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import shapeless.ops.product.ToMap

object Crimes1 {
  
  // Hash function to convert string to hash code
  def knuthHash(word: String, constant: Int): Int = {
    var hash = 0
    for (ch <- word.toCharArray) {
      hash = ((hash << 5) ^ (hash >> 27)) ^ ch.toInt
    }
     
    hash % constant  
  }
  
  def knuthHashInt(word: Byte, constant: Int): Int = {
    var hash = 0
    for (ch <- word.toString().toCharArray()) {
      hash = ((hash << 5) ^ (hash >> 27)) ^ ch.toInt
    }
     
    hash % constant  
  }
  


  // Calculating squared distance between a record and a cluster centroid
  def calculateDistance(record: (Double,Double,Double,Double),centroid: ((Double,Double,Double,Double))) = {
    (Math.pow(record._1 - centroid._1, 2) + 
    Math.pow(record._2 - centroid._2, 2) + 
    Math.pow(record._3 - centroid._3, 2) +
    Math.pow(record._4 - centroid._4, 2))
  }

  // Adding records for each cluster to measure weight of each cluster
  def add(record1: (Double,Double,Double,Double),record2: ((Double,Double,Double,Double))) = {
	    (record1._1 + record2._1,
	    record1._2 + record2._2,
	    record1._3 + record2._3,
	    record1._4 + record2._4)
	  }
	  
  // Finding the cluster centroid to which the record is closest to and returning the cluster id
  def closestToCluster(record: (Double, Double, Double, Double), clusters: Array[(Double, Double, Double, Double)]): Int = {
		  
    var clusterAssigned = 0	  
		var minimumDistance = Double.PositiveInfinity

	  for (i <- 1 to clusters.length) {
		  var distanceToCentroid = calculateDistance(record, clusters(i-1))
				  if (distanceToCentroid < minimumDistance) {
					  minimumDistance = distanceToCentroid
							  clusterAssigned = i
				  }
	  }
    clusterAssigned
  }
	  
	  /*def computeCost(clusters: RDD[((Double,Double,Double,Double),Int)], records: RDD[(Int,((Double,Double,Double,Double),Int))]):
	  Double = {
	    var clustSum = 0.0
	    var interClusterSum = 0.0
	    clusters.foreach(cluster=> {
	      var filteredRecords = records.filter(f=> f._1 == cluster._2)
	      records.foreach(record => {
	        if(cluster._2 == record._1) {
	          clustSum += calculateDistance(record._2._1, cluster._1)
	        }
	      })
	      interClusterSum += clustSum
	    })
	    
	    interClusterSum
	    
	  }*/
	  
  def runKmeans(k: Int,transformInput: RDD[(Double,Double,Double,Double)],sc: SparkContext): 
  (RDD[((Double,Double,Double,Double),Int)],RDD[(Int)]) = { 
    
	  // Initializing the RDD to assign the cluster to each point
		var closestTo = transformInput.map(x => (0,(x,0)))
		
		// Randomly initialize cluster centroids
		var clusters = transformInput.takeSample(false,k,System.nanoTime())
		
		
	  var currentIteration = 1
	  val convergence = 0.01
	  var dist = Double.PositiveInfinity
		
	  while ( dist >  convergence) {
		  
		  closestTo = transformInput.map(record => { 
		    var clusterAssigned = closestToCluster(record, clusters)
		    (clusterAssigned, (record,1))
		    
		  })
		 
		  val calculateClusters = closestTo.reduceByKey {
		    case ((record1,r1), (record2,r2)) => (add(record1,record2),r1+r2) 
		  }
		  
		  var calculateNewClusterCentroid = calculateClusters.map { 
		    case(clusterIndex, (clusterInfo,totalRecords)) => (clusterIndex, (clusterInfo._1/totalRecords,
		        clusterInfo._2/totalRecords,
		        clusterInfo._3/totalRecords,
		        clusterInfo._4/totalRecords))
		  }.collectAsMap()
		  
		  
		  /*println("Iteration number: " + currentIteration + "\n")
		  calculateNewClusterCentroid.foreach(println)
		  
		  println("\n\n\n\nIteration " + currentIteration + ": ")
		  
		  println("\n\nInitials Clusters: ")
		  clusters.foreach(println)
		  
		  println("\n\nCalculated: ")
		  calculateNewClusterCentroid.foreach(println)*/
		  
		  
		  dist = 0.0
		  for (i <- 1 to k) {
        dist += calculateDistance(clusters(i-1), calculateNewClusterCentroid(i))
      }
		  
		  
		  // Assign new cluster centroids
		  for(i <- 1 to clusters.length) {
		    clusters(i-1) = calculateNewClusterCentroid(i)
		  }
		  
		  //println("\n\nNew Clusters: ")
		  //clusters.foreach(println)

		  currentIteration+=1
	  }
	  		
		// Assigning cluster ID's to each cluster
		var i = 0
		val clust = sc.parallelize(clusters).map(f=> {
		  i+=1
		  ((f._1,f._2,f._3,f._4),i)
		})
		
		//val cost = computeCost(clust,closestTo)
		
		(clust,closestTo.map(f=>f._1))
  }
  
  def calculateMinDist(cluster: RDD[((Double,Double,Double,Double),Int)], 
      clusterRecords: RDD[(((String,String,Int,Int),(Double,Double,Double,Double)),Int)],sc: SparkContext) = {
    
   
    
    // Max and Min represents the maximum and the minimum distance from the cluster centroid
    var minTime = Double.PositiveInfinity
    var minCrimeType = Double.PositiveInfinity
    var minArea = Double.PositiveInfinity
    var minCommunity = Double.PositiveInfinity

    // Finding the highest crime type committed
    var clust2 = cluster.collect()
    val clu2 = sc.broadcast(clust2)
    var highestCrimeType = clusterRecords.map(record => {
      var crim = clu2.value
      var c = crim(0)._1._2
      var crime = record._1._2._2 - c
      var crimeType = ""
      if(crime < minCrimeType) {
        minCrimeType = crime
        crimeType = record._1._1._2
        (crimeType)
      } else {
        ("delete")
      }
    })
    highestCrimeType = highestCrimeType.filter(f=> f!="delete")
    var highestCrime = highestCrimeType.take(highestCrimeType.count().toInt).drop(((highestCrimeType.count().toInt) - 1))
    println("Crime Type: " + highestCrime(0).toString())
    
    // Finding the most common time
    var clust1 = cluster.collect()
    val clu1 = sc.broadcast(clust1)
    var mostCommonTime = clusterRecords.map(record => {
      var commonTime = clu1.value
      var time = commonTime(0)._1._1
      var timeDiff = record._1._2._1 - time
      var commonT = ""
      if(timeDiff < minTime) {
        minTime = timeDiff
        commonT = record._1._1._1
        (commonT)
      } else {
        ("delete")
      }
    })
    mostCommonTime = mostCommonTime.filter(f=> f!="delete")
    var commonTime = mostCommonTime.take(mostCommonTime.count().toInt).drop(((mostCommonTime.count().toInt) - 1))
    println("Most common time at which crime occurs: " + commonTime(0).toString())
    
    //Finding the most common area code where crime occurs
    var clust3 = cluster.collect()
    val clu3 = sc.broadcast(clust3)
    var mostCommonArea = clusterRecords.map(record => {
      var commonArea = clu3.value
      var area = commonArea(0)._1._3
      var areaDiff = record._1._2._3 - area
      var commonA = 0
      if(areaDiff < minArea) {
        minArea = areaDiff
        commonA = record._1._1._3
        (commonA)
      } else {
        (-1)
      }
    })
    mostCommonArea = mostCommonArea.filter(f=> f!= -1)
    var commonArea = mostCommonArea.take(mostCommonArea.count().toInt).drop(((mostCommonArea.count().toInt) - 1))
    println("Most common area code where crime occurs: " + commonArea(0))
    
    //finding the most common community code where crime occurs
    var clust4 = cluster.collect()
    val clu4 = sc.broadcast(clust4)
    var mostCommonCommunity = clusterRecords.map(record => {
      var commonCommunity = clu4.value
      var community = commonCommunity(0)._1._4
      var communityDiff = record._1._2._4 - community
      var commonC = 0
      if(communityDiff < minCommunity) {
        minCommunity = communityDiff
        commonC = record._1._1._4
        (commonC)
      } else {
        (-1)
      }
    })
    mostCommonCommunity = mostCommonCommunity.filter(f=> f!= -1)
    var commonCommunity = mostCommonCommunity.take(mostCommonCommunity.count().toInt).drop(((mostCommonCommunity.count().toInt) - 1))
    println("Most common community code where crime occurs: " + commonCommunity(0))
  }


	def main(args: Array[String]) {	  
	  
	  // Defining spark context and configuration
	  val conf = new SparkConf().setAppName("Crimes").setMaster("local[1]")
		val sc = new SparkContext(conf)
		
		// Reading Input and filtering out any rows that has a null value
		var input = sc.textFile(args(0)).map(line=>{
		  var x = line.split(",") 
		  if(x.length == 4) {
		    
		    // Cleaning time dimension to proper value
		    var time = x(0).toString().split(" ") 
		    var continuousTime = time(1).toString().split(":")
		    var finalTime = ""
		    if(time(2).toString() == "PM" && continuousTime(0).toInt != 12) {
		      finalTime = (continuousTime(0).toInt + 12).toString() + ":" + continuousTime(1) + ":" + continuousTime(2)
		    } else if(time(2).toString() != "PM" && continuousTime(0).toInt == 12) {
		      finalTime = (continuousTime(0).toInt - 12).toString() + ":" + continuousTime(1) + ":" + continuousTime(2)
		    } else {
		      finalTime = continuousTime(0) + ":" + continuousTime(1) + ":" + continuousTime(2)
		    }
		    
		    (finalTime,x(1).toString(),x(2).toInt,x(3).toInt)
		  } else {
		    (null,null,null.asInstanceOf[Int],null.asInstanceOf[Int])
		  }
		}).filter(x => x._1 != null)
		
		
		var transformInput = input.map(input => {
		  var time = knuthHash(input._1, Int.MaxValue).toDouble
		  var crimeType = input._2.map(_.toByte.doubleValue()).reduceLeft((x,y) => x+y)
		  var district = knuthHashInt(input._3.toByte,Int.MaxValue).toDouble
		  var community = input._4.toDouble
		  (time,crimeType,district,community)
		})
		
		var returnValue = runKmeans(10,transformInput, sc)
		
		val clusters = returnValue._1
		val recordsAssignedToCluster = returnValue._2
		//val cost = returnValue._3
	
		val temp = input.zip(transformInput)
		val result = temp.zip(recordsAssignedToCluster)

		clusters.foreach(println)
		
		var cluster1 = clusters.filter(f=> f._2 == 10)
		cluster1.foreach(println)
		var cluster1records = result.filter(f=> f._2 == 10)
		
		var crimet = calculateMinDist(cluster1,cluster1records,sc)
		
	  
		sc.stop()
	}
}

// k2 = 