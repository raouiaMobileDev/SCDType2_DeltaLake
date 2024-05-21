package com.databeans.utils

import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.databeans.models.{History, _}

class UtilsSpec extends AnyFlatSpec with Matchers with GivenWhenThen {

  implicit val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .appName("HistoryBuilderDataFrame_Test")
    .getOrCreate()

  import spark.implicits._

  "BuildResultSameAddress" should "Trait data within the DataFrame" in {
    Given("The input dataframes")
    val inputHistoryData = Seq(
      History(5, "Bilel", "Ben amor", "Sfax", "2010-12-01", "2022-12-01",false),
      History(1, "Iheb", "Ben amor", "Sfax", "2010-05-01", "2012-07-01",false)
    ).toDF()
    val inputUpdateData = Seq(
      Update(5, "Bilel", "Ben amor", "Sfax", "2021-12-12"),
      Update(15, "Salah", "Ben amor", "Tunis", "2024-1-12")
    ).toDF()
    When("buildResult function is invoked")
    val utils = new Utils(spark, inputHistoryData, inputUpdateData)
    val resultBuildData =  utils.SCDType2Delta()
    Then("The dataframe should be returned")
    val expectedResultData = Seq(
      History(5, "Bilel", "Ben amor", "Sfax", "2010-12-01", "2021-12-12",false),
      History(5, "Bilel", "Ben amor", "Sfax", "2021-12-12", "2022-12-01",false),
      History(15, "Salah", "Ben amor", "Tunis", "2024-1-12", null, true)
    ).toDF()
        expectedResultData.collect() should contain theSameElementsAs (resultBuildData.collect())
  }

  "BuildResultDifferentAddress" should "Trait data within the DataFrame" in {
    Given("The input dataframes")
    val inputHistoryData = Seq(
      History(10, "Mohsen", "Abidi", "Sousse", "1995-12-23", "2020-12-12", false),
      History(1, "Iheb", "Ben amor", "Sfax", "2010-05-01", "2012-07-01", false),
    ).toDF()
    val inputUpdateData = Seq(
      Update(10, "Mohsen", "Abidi", "Tunis", "2021-01-01"),
    ).toDF()
    When("buildResult function is invoked")
    val utils = new Utils(spark, inputHistoryData, inputUpdateData)
    val resultBuildData =  utils.SCDType2Delta()
    Then("The dataframe should be returned")
    val expectedResultData = Seq(
      History(10, "Mohsen", "Abidi", "Sousse", "1995-12-23", "2020-12-12", false),
      History(10, "Mohsen", "Abidi", "Tunis", "2021-01-01", null, true),
    ).toDF()
    expectedResultData.collect() should contain theSameElementsAs (resultBuildData.collect())
  }


  "BuildResultSameAddressWithPeriod" should "Trait data within the DataFrame" in {
    Given("The input dataframes")
    val inputHistoryData = Seq(
      History(1, "Iheb", "Ben amor", "Sfax", "2010-05-01", "2012-07-01", false),
      History(4, "Mohamed", "Ben amor", "Sfax", "2015-12-01", "2016-12-01", false),
    ).toDF()
    val inputUpdateData = Seq(
      Update(4, "Mohamed", "Ben amor", "Sfax", "2016-12-01")
    ).toDF()
    When("buildResult function is invoked")
    val utils = new Utils(spark, inputHistoryData, inputUpdateData)
    val resultBuildData =  utils.SCDType2Delta()
    Then("The dataframe should be returned")
    val expectedResultData = Seq(
      History(4, "Mohamed", "Ben amor", "Sfax", "2015-12-01", "2016-12-01", false),
    ).toDF()
    expectedResultData.collect() should contain theSameElementsAs (resultBuildData.collect())
  }

}