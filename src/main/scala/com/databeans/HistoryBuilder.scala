package com.databeans

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import com.databeans.utils.Utils


object HistoryBuilder {

  def historyBuilder(spark: SparkSession, inputHistoryData: DataFrame, inputUpdateData: DataFrame): DataFrame = {
    val utils = new Utils(spark, inputHistoryData, inputUpdateData)
    val resultHistoryBuilder =  utils.SCDType2Delta()
    val resultFromPickedHistory = inputHistoryData.as("history").join(resultHistoryBuilder.as("resultHistoryBuilder"), Seq("id"), "left_anti")
      .select(
        col("history.id"),
        col("history.firstName"),
        col("history.lastName"),
        col("history.address").as("address"),
        col("history.moved_in").as("moved_in"),
        col("history.moved_out").as("moved_out"),
        col("history.current").as("current")
      )
    resultHistoryBuilder.union(resultFromPickedHistory)
  }

}
