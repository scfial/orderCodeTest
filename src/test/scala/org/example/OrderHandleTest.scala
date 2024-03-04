import com.example.OrderHandle.givedPeerIdAndMaxTotalCumulative
import com.example.OrderHandle.findFirstYearOfPeerId
import com.example.OrderHandle.sumPeerIdByGivenYear
import com.example.OrderHandle.givedPeerIdAndMaxTotalCumulativeFinal
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import junit.framework.Assert
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.FunSuite
import org.scalatest.mockito.MockitoSugar

class OrderHandleTest extends FunSuite with SparkSessionTestWrapper with MockitoSugar
  with DataFrameComparer with Serializable {

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

  test("testGivedPeerIdAndMaxTotalCumulativeFinal") {
    val finalData = Seq(Row("AE686(AE)", "7", "AE686", 2022),
      Row("AE686(AE)", "8", "BH2740", 2021),
      Row("AE686(AE)", "9", "EG999", 2021),
      Row("AE686(AE)", "10", "AE0908", 2023),
      Row("AE686(AE)", "11", "QA402", 2022),
      Row("AE686(AE)", "12", "OA691", 2022),
      Row("AE686(AE)", "12", "OB691", 2022),
      Row("AE686(AE)", "12", "OC691", 2019),
      Row("AE686(AE)", "12", "OD691", 2017)
    )
    val dataFrameFinal = spark.createDataFrame(spark.sparkContext.parallelize(finalData),structType)
    val actualData = givedPeerIdAndMaxTotalCumulativeFinal(dataFrameFinal,"AE686(AE)",5)
    val expectedData = Array(
      ("AE686(AE)",2022),
      ("AE686(AE)",2021)
    )
    Assert.assertTrue(actualData.sameElements(expectedData))
  }
  test("testFindFirstYearOfPeerId") {
    val actualData = findFirstYearOfPeerId(dataFrame,"ABC17969(AB)")
    val expectedData = 2022
    Assert.assertTrue(actualData==expectedData)
  }
  test("testSumPeerIdByGivenYear") {
    val actualData = sumPeerIdByGivenYear(dataFrame,"ABC17969(AB)")
    val expectedData = Array(
      (2022,4L),
      (2021,1L)
    )
    Assert.assertTrue(actualData.sameElements(expectedData))
  }
  test("testGivedPeerIdAndMaxTotalCumulative") {
    val actualData = givedPeerIdAndMaxTotalCumulative(dataFrame,"AE686(AE)",3)
    val expectedData = Array(
      (2023,1L),
      (2022,2L)
    )
    Assert.assertTrue(actualData.sameElements(expectedData))
    spark.stop()
  }
}
trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("spark test example")
      .config("spark.driver.bindAddress","127.0.0.1")
      .getOrCreate()
  }

}