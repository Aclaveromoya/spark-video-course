package es.azaharaclavero.sparkvideocourse

import es.azaharaclavero.sparkvideocourse.utils.SparkConfiguration
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object WindowsFunctionsMain extends SparkConfiguration {
  def main(args: Array[String]): Unit = {
    val sc = spark.sparkContext
   // sc.parallelize()
    import spark.implicits._

    // Ejemplo de datos
    val data = Seq(
      ("Alice", 1000),
      ("Bob", 1500),
      ("Alice", 2000),
      ("Bob", 1200),
      ("Alice", 3000)
    )

    val df = data.toDF("vendedor", "ventas")

    // Definir la ventana
    val windowSpec = Window.partitionBy("vendedor")

    // Calcular la suma acumulada
    val result = df.withColumn("suma_acumulada", sum("ventas").over(windowSpec))
    df.show()
    result.show()

    df.groupBy("vendedor").agg(sum("ventas").as("total_ventas")).show()
    Thread.sleep(300000)

  }
}
