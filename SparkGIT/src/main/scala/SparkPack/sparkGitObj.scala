package SparkPack

import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


object sparkGitObj {
  
  	def main(args:Array[String]):Unit={ 
	  
	
			val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")

					val spark=SparkSession.builder().getOrCreate()					
					import spark.implicits._
					
					
					val jsonDF = spark.read.format("json").option("multiLine",true).load("file:///D://MyData/reqapi.json")
					
					jsonDF.show()
					jsonDF.printSchema()
					
					val flattenDF = jsonDF.select(
					                              col("data.avatar").alias("data_avatar"),
					                              col("data.email").alias("data_email"),
					                              col("data.first_name").alias("data_first_name"),
					                              col("data.id").alias("data_id"),
					                              col("data.last_name").alias("data_last_name"),
					                              col("page"),
					                              col("per_page"),
					                              col("support.text").alias("support_text"),
					                              col("support.url").alias("support_url"),
					                              col("total"),
					                              col("total_pages")	    
					    
					                              )
					flattenDF.show()
					flattenDF.printSchema()
					
					
					val complexDF = flattenDF.select(
					                          struct(
					                            col("data_avatar").alias("avatar"),
					                            col("data_email").alias("email"),
					                            col("data_first_name").alias("first_name"),
					                            col("data_id").alias("id"),
					                            col("data_last_name").alias("last_name")
					                          ).alias("data"),
					                          col("page"),
					                          col("per_page"),
					                          struct(
					                            col("support_text").alias("text"),
					                            col("support_url").alias("url")
					                          ).alias("support"),
					                          col("total")	,
					                          col("total_pages")
					                                 )
					                                
					complexDF.show()
					complexDF.printSchema()
					
					//// code updated
					
  	}
}