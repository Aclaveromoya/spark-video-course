package es.azaharaclavero.sparkvideocourse

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object TransforAndOptimi {
  def main(args: Array[String]): Unit = {
    // Crear una sesión de Spark
    val spark = SparkSession.builder()
      .appName("Spark Video Course")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val sc = spark.sparkContext

    // Datos de ejemplo para el DataFrame 1
    val data1 = Seq(
      ("Alice", 25, "F"),
      ("Bob", 30, "M"),
      ("Charlie", 22, "M"),
      ("Diana", 28, "F")
    )

    // Datos de ejemplo para el DataFrame 2
    val data2 = Seq(
      ("Alice", "New York"),
      ("Bob", "San Francisco"),
      ("Charlie", "Los Angeles"),
      ("Eve", "Chicago")
    )

    // Crear DataFrames
    val columns1 = Seq("name", "age", "gender")
    val df1 = spark.createDataFrame(data1).toDF(columns1: _*)

    val columns2 = Seq("name", "city")
    val df2 = spark.createDataFrame(data2).toDF(columns2: _*)

    // Aplicar Predicate Pushdown (Filtrado)
    val filtered_df = df1.filter(col("age") > 25)

    // Aplicar Constant Folding
    val folded_df = filtered_df.select(col("name"), (col("age") + 2).as("age_plus_2"))

    // Aplicar Column Pruning
     val pruned_df = folded_df.select(col("name"))

    // Reordenar Join
        val reordered_join = df1.join(df2, "name")

    // Mostrar los resultados finales
    println("Predicate pushdown: Pushing filtering conditions closer to the data source before processing to minimize data movement.")
    println("Filtered DataFrame:")
    filtered_df.show()

    println("Constant folding: Evaluating constant expressions during query compilation to reduce computation during runtime.")
    println("Folded DataFrame:")
    folded_df.show()

    println("Column pruning: Eliminating unnecessary columns from the query plan to enhance processing efficiency.")
    println("Pruned DataFrame:")
    pruned_df.show()

    println("Join reordering: Rearranging join operations to minimize the intermediate data size and enhance the join performance.")
    println("Reordered Join DataFrame:")
    reordered_join.show()

    // Detener la sesión de Spark
    spark.stop()

  }
}
