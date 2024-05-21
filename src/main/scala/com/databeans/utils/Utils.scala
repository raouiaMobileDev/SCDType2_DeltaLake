package com.databeans.utils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import io.delta.tables._

class Utils(spark: SparkSession,x: DataFrame, y: DataFrame) {

  def SCDType2Delta (): DataFrame = {

    val takeHistory = y.as("update")
      .join(x.as("history"), x("id") === y("id"))
      .select(
        col("history.id"),
        col("history.firstName"),
        col("history.lastName"),
        col("history.address"),
        col("history.moved_in"),
        col("history.moved_out"),
        col("history.current"),
      )

    val adjustedUpdate = y
      .withColumn("moved_out", lit(null))
      .withColumn("current", lit(true))

    var unionHistoryAndUpdate=takeHistory.union(adjustedUpdate)
    var sourceUpdate= takeHistory.as("history")
      .join(adjustedUpdate.as("update"), takeHistory("id") === adjustedUpdate("id"))
      .select(
        col("update.id"),
        col("update.firstName"),
        col("update.lastName"),
        col("update.address"),
        col("update.moved_in"),
        col("history.moved_out") as ("history_moved_out"),
        col("update.current"),
        col("history.address") as ("history_address")
      )
    unionHistoryAndUpdate.write.format("delta").mode("overwrite").save("path/to/delta_table")
    val unionHistoryAndUpdateDelta = DeltaTable.forPath("path/to/delta_table")
    unionHistoryAndUpdateDelta.as("target").merge(
        sourceUpdate.as("source"),
        "target.id = source.id"
      )
      .whenMatched("target.address = source.address and target.moved_in<source.moved_in and target.moved_out > source.moved_in" )
      .updateExpr(Map(
                "target.moved_out" -> "source.moved_in"
              ))
      .whenMatched("target.address = source.address and target.address=source.history_address and target.moved_in=source.moved_in and target.moved_in<source.history_moved_out and target.moved_out IS NULL" )
      .updateExpr(Map(
        "target.moved_out" -> "source.history_moved_out",
        "target.current" -> "false"
      ))
      .whenMatched("target.address = source.address and target.address=source.history_address and  target.moved_in=source.history_moved_out and target.moved_out IS NULL" )
      .delete()
      .execute()

    unionHistoryAndUpdateDelta.toDF
  }
}
