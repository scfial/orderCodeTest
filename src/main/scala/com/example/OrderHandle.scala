package com.example

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
object OrderHandle {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Order handle")
      .master("local[2]")
      .getOrCreate()


    val data = Seq(Row("ABC17969(AB)", "1", "ABC17969", 2022),
      Row("ABC17969(AB)", "2", "CDC52533", 2022),
      Row("ABC17969(AB)", "3", "DEC59161", 2023),
      Row("ABC17969(AB)", "4", "F43874", 2022),
      Row("ABC17969(AB)", "5", "MY06154", 2021),
      Row("ABC17969(AB)", "6", "MY4387", 2022),
      Row("AE686(AE)", "7", "AE686", 2023),
      Row("AE686(AE)", "8", "BH2740", 2021),
      Row("AE686(AE)", "9", "EG999", 2021),
      Row("AE686(AE)", "10", "AE0908", 2021),
      Row("AE686(AE)", "11", "QA402", 2022),
      Row("AE686(AE)", "12", "OM691", 2022)
    )
    val structType = StructType(Seq(StructField("peer_id",StringType),
      StructField("id_1",StringType),
      StructField("id_2",StringType),
      StructField("year",IntegerType)))
    val rdd = spark.sparkContext.parallelize(data)
    val dataFrame = spark.createDataFrame(rdd,structType)
    givedPeerIdAndMaxTotalCumulative(dataFrame,"AE686(AE)",3).foreach(println)
    spark.stop()
  }

  def givedPeerIdAndMaxTotalCumulativeFinal(rdd: DataFrame, peerId: String, maxTotal: Int): Array[(String,Int)] = {
    val peerIdRdd = filterPeerId(rdd, peerId)
    val year = getFirstYear(peerIdRdd)
    specifyCumulative(sumByGivenYear(peerIdRdd, year), maxTotal).collect().map(f=>(f.getString(0),f.getInt(1)))
  }
  def givedPeerIdAndMaxTotalCumulative(rdd: DataFrame, peerId: String, maxTotal: Int): Array[(Int,Long)] = {
    val peerIdRdd = filterPeerId(rdd, peerId)
    val year = getFirstYear(peerIdRdd)
    specifyCumulative(sumByGivenYear(peerIdRdd, year), maxTotal).collect().map(f=>(f.getInt(1),f.getLong(2)))
  }
  def findFirstYearOfPeerId(rdd:DataFrame,peerId:String):Int= {
    val peerIdRdd = filterPeerId(rdd, peerId)
    getFirstYear(peerIdRdd)
  }
  def getFirstYear(rdd:DataFrame):Int= {
    import org.apache.spark.sql.functions._
    rdd.withColumn("id_1", col("id_1").cast("integer")).orderBy("id_1").take(1).head.getInt(3)
  }

  def sumPeerIdByGivenYear(rdd:DataFrame,peerId: String):Array[(Int,Long)]={
    val peerIdRdd = filterPeerId(rdd, peerId)
    val year = getFirstYear(peerIdRdd)
    sumByGivenYear(peerIdRdd, year).collect().map(f=>(f.getInt(1),f.getLong(2)))
  }
  def sumByGivenYear(rdd:DataFrame,year:Int):DataFrame={
    rdd.filter(_.getInt(3)<=year).groupBy("peer_id","year").count()
  }
  def specifyCumulative(rdd:DataFrame,maxTotal:Int): DataFrame = {
    import org.apache.spark.sql.functions._
    val winCum = Window.orderBy(desc("year"))
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    val winLag = Window.orderBy(desc("year"))
    rdd.withColumn("cumCount", sum("count").over(winCum))
      .withColumn("perCount",lag("count",1).over(winLag)).filter(f=>{
        f.getLong(3)<=maxTotal||f.getLong(3)-f.getLong(2)<maxTotal
      }).drop("cumCount","perCount")
  }
  def filterPeerId(rdd:DataFrame,peerId:String):DataFrame={
    rdd.filter(_.getString(0).contains(peerId))
  }
}
