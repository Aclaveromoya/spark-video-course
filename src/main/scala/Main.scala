package es.azaharaclavero.sparkvideocourse

import es.azaharaclavero.sparkvideocourse.utils.SparkConfiguration
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.logging.log4j.core.config.Configurator



object Main  extends SparkConfiguration {
  /**
   * Este es el punto de entrada principal para la aplicación Spark.
   * Crea un SparkSession y ejecuta algunas operaciones de ejemplo.
   */

case class Producto(fecha: String, nombre: String, cantidad: Int)
  def main(args: Array[String]): Unit = {
    import spark.implicits._

    val sc = spark.sparkContext
    println("COMENZAMOS TRABAJOS CON RDD")
    // Crear un RDD de ejemplo
    val ventasRDD = sc.parallelize(Seq(
      ("2023-01-01", "Producto A", 100),
      ("2023-01-02", "Producto B", 200),
      ("2023-01-03", "Producto C", 300),
      ("2023-01-01", "Producto A", 150),
      ("2023-01-02", "Producto B", 250),
      ("2023-01-03", "Producto C", 350),
      ("2023-01-01", "Producto A", 200),
      ("2023-01-02", "Producto B", 300),
      ("2023-01-03", "Producto C", 200)
    ))
    val ventasPorProducto = ventasRDD.map { case (_, product, amount) => (product, amount) }
      .reduceByKey(_ + _)

    println("El resultado del RDD ventasPorProducto es:")
    ventasPorProducto.collect().foreach(println)

    // Contar el número de elementos
    val countRDD = ventasRDD.count()
    println(s"Total elementos en RDD: $countRDD")

    // Filtrar elementos
    val filteredRDD = ventasRDD.filter { case (_, product, _) => product == "Producto A" }
    println("El resultado del RDD filteredRDD es:")
    filteredRDD.collect().foreach(println)

    // Mapear a una nueva estructura
    val mappedRDD = ventasRDD.map { case (date, product, amount) => (product, amount * 2) }
    println("El resultado del RDD mappedRDD es:")
    mappedRDD.collect().foreach(println)

    // Agregar por clave
    val aggregatedRDD = ventasRDD.map { case (_, product, amount) => (product, amount) }
      .reduceByKey(_ + _)
    println("El resultado del RDD aggregatedRDD es:")
    aggregatedRDD.collect().foreach(println)

    // Serialización personalizada
    val serializedRDD = ventasRDD.mapPartitions { iter =>
      val serialized = iter.map { case (date, product, amount) => s"$date,$product,$amount" }
      serialized
    }
    println("El resultado del RDD serializedRDD es:")
    serializedRDD.collect().foreach(println)


    //---------------------------DATAFRAME---------------------------
    println("COMENZAMOS TRABAJOS CON DATAFRAME")
    // Crear un DataFrame de ejemplo
    val ventasDF = Seq(
      ("2023-01-01", "Producto A", 100),
      ("2023-01-02", "Producto B", 200),
      ("2023-01-03", "Producto C", 300),
      ("2023-01-01", "Producto A", 150),
      ("2023-01-02", "Producto B", 250),
      ("2023-01-03", "Producto C", 350),
      ("2023-01-01", "Producto A", 200),
      ("2023-01-02", "Producto B", 300),
      ("2023-01-03", "Producto C", 200)
    ).toDF("fecha", "producto", "cantidad")

    val ventasPorProductoDF = ventasDF
      .groupBy("producto")
      .agg(sum("cantidad").as("total_cantidad"))

    println("El resultado del DataFrame ventasPorProductoDF es:")
    ventasPorProductoDF.show(false)

    // Contar el número de elementos
    val countDF = ventasDF.count()
    println(s"Total elementos en DataFrame: $countDF")

    // Filtrar elementos
    val filteredDF = ventasDF.filter($"producto" === "Producto A")
    println("El resultado del DataFrame filteredDF es:")
    filteredDF.show(false)

    // Mapear a una nueva estructura
    val mappedDF = ventasDF.withColumn("cantidad_doble", $"cantidad" * 2)
    println("El resultado del DataFrame mappedDF es:")
    mappedDF.show(false)

    // Agregar por columna
    val aggregatedDF = ventasDF.groupBy("producto").agg(sum("cantidad").as("total_cantidad"))
    println("El resultado del DataFrame aggregatedDF es:")
    aggregatedDF.show(false)



    // Serialización a JSON
    val jsonDF = ventasDF.toJSON
    println("El resultado del DataFrame jsonDF es:")
    jsonDF.show(false)



    //---------------------------DATASET---------------------------


    // Crear un DataSet de ejemplo
  println("COMENZAMOS TRABAJOS CON DATASET")
    val ds = Seq(
     Producto("2023-01-01", "Producto A", 100),
     Producto("2023-01-02", "Producto B", 200),
     Producto("2023-01-03", "Producto C", 300),
     Producto("2023-01-01", "Producto A", 150),
     Producto("2023-01-02", "Producto B", 250),
     Producto("2023-01-03", "Producto C", 350),
     Producto("2023-01-01", "Producto A", 200),
     Producto("2023-01-02", "Producto B", 300),
     Producto("2023-01-03", "Producto C", 200)
    ).toDS()

    println("El resultado del dataset DS es:")
    ds.show(false)

    // Contar el número de elementos
    val countDS = ds.count()
    println(s"Total elementos en DataSet: $countDS")

    // Filtrar elementos
    val filteredDS = ds.filter(_.nombre == "Producto A")
    println("El resultado del dataset filteredDS es:")
    filteredDS.show(false)

    // Mapear a una nueva estructura
    val mappedDS = ds.map(producto => producto.copy(cantidad = producto.cantidad * 2))
    println("El resultado del dataset mappedDS es:")
    mappedDS.show(false)
    // Agregar por campo
    val aggregatedDS = ds.groupByKey(_.nombre).agg(sum("cantidad").as[Int])
    println("El resultado del dataset aggregatedDS es:")
    aggregatedDS.show(false)

    // Serialización a JSON
    val jsonDS = ds.toJSON
    println("El resultado del dataset jsonDS es:")
    jsonDS.show(false)

    // Crear dataset a partir de un archivo de texto
    val datosDS = spark.read.text("data/datos.txt").as[String]
    println("El resultado del dataset datosDF es:")
    datosDS.show(false)
    Thread.sleep(300000)
    /*
        val df: DataFrame = spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv("C:\\Repositories\\workSpace\\spark-video-course\\data\\ABC.csv")

        df.show(5)
        df.printSchema()

        // Select specific columns
        df.select( "Open", "Close","Date").show(5)
        val colunm = df("Date")
        col("Date")
        import spark.implicits._
        $"Date"

        df.select(col("Date"), $"Open",df("Close")).show(2)

        val column = df("Open")
        val newColumn = column.plus(2.0)
        val columnString = column.cast(StringType)
    */

  }
}
