package com.spark.appStreaming

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.annotation.tailrec

object genricFunctions {
  implicit class DFHelpers(df: DataFrame) {
    def columns = {
      val dfColumns = df.columns.map(_.toLowerCase)
      df.schema.fields.flatMap { data =>
        data match {
          case column if column.dataType.isInstanceOf[StructType] => {
            column.dataType.asInstanceOf[StructType].fields.map { field =>
              val columnName = column.name
              val fieldName = field.name
              col(s"${columnName}.${fieldName}").as(s"${columnName}_${fieldName}")
            }.toList
          }
          case column => List(col(s"${column.name}"))
        }
      }
    }

    def flatten: DataFrame = {
      val empty = df.schema.filter(_.dataType.isInstanceOf[StructType]).isEmpty
      empty match {
        case false =>
          df.select(columns: _*).flatten
        case _ => df
      }
    }
    //Tail rec function to keep exploding the array fields
    def explodeColumns = {
      @tailrec
      def columns(cdf: DataFrame): DataFrame = cdf.schema.fields.filter(_.dataType.typeName == "array") match {
        case c if !c.isEmpty => columns(c.foldLeft(cdf)((dfa, field) => {
          dfa.withColumn(field.name, explode_outer(col(s"${field.name}"))).flatten
        }))
        case _ => cdf
      }

      columns(df.flatten)
    }
  }
}