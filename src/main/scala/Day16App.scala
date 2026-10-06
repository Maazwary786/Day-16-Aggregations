import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Day16App {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day16-Aggregations")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val hospital = Seq(
      ("D1", "Cardiology", 10000.0),
      ("D1", "Cardiology", 15000.0),
      ("D2", "Neurology", 12000.0),
      ("D2", "Neurology", 18000.0),
      ("D3", "Orthopedic", 8000.0),
      ("D3", "Orthopedic", 11000.0)
    ).toDF("patient_id", "department", "revenue")

    hospital.agg(
      count("*").alias("records"),
      sum("revenue").alias("total_revenue"),
      avg("revenue").alias("avg_revenue"),
      min("revenue").alias("min_revenue"),
      max("revenue").alias("max_revenue")
    ).show()

    val departmentStats = hospital
      .groupBy("department")
      .agg(
        count("*").alias("patients"),
        sum("revenue").alias("revenue"),
        avg("revenue").alias("average_revenue")
      )
      .filter($"revenue" > 20000)

    departmentStats.show()

    spark.stop()
  }
}
