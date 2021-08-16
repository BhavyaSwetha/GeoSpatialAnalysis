package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{
  def stcontains(queryRectangle: String, pointString: String) : Boolean = {

    val x1 = queryRectangle.split(",")(0).trim().toDouble
    val y1 = queryRectangle.split(",")(1).trim().toDouble
    val x2 = queryRectangle.split(",")(2).trim().toDouble
    val y2 = queryRectangle.split(",")(3).trim().toDouble

    val x = pointString.split(",")(0).trim().toDouble
    val y = pointString.split(",")(1).trim().toDouble

    var rectminx = x1
    var rectmaxx = x2

    if (x1 > x2) {
      rectminx = x2
      rectmaxx = x1
    }

    var rectminy = y1
    var rectmaxy = y2

    if (y1 > y2) {
      rectmaxy = y1
      rectminy = y2
    }

    if ((rectminx <= x) && (rectmaxx >= x) && (rectminy <= y) && (rectmaxy >= y)) {
      return true
    } else {
      return false
    }

  }

  def stwithin(pointString1: String, pointString2: String, distance: Double) : Boolean = {

    val x1 = pointString1.split(",")(0).trim().toDouble
    val y1 = pointString1.split(",")(1).trim().toDouble
    
    val x2 = pointString2.split(",")(0).trim().toDouble
    val y2 = pointString2.split(",")(1).trim().toDouble

    val xs = scala.math.pow((x1-x2), 2)
    val ys = scala.math.pow((y1-y2), 2)

    val d = scala.math.pow((xs + ys), 0.5)

    if (d <= distance) {
      return true
    } else {
      return false
    }
  }
  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>(stcontains(queryRectangle, pointString)))

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>(stcontains(queryRectangle, pointString)))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>(stwithin(pointString1, pointString2, distance)))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>(stwithin(pointString1, pointString2, distance)))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }
}
