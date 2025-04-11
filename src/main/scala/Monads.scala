package es.azaharaclavero.sparkvideocourse

// Ejemplo de Monadas en Scala
// Este código muestra ejemplos de las monadas Option, Try, Future, Either y List en Scala.

import scala.util.{Either, Left, Right}
import scala.util.{Try, Success, Failure}
import scala.concurrent.{Future, Await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object Monads {
  def main(args: Array[String]): Unit = {
    // Ejemplo de Option
    println("Option Monad:")
    def divideOption(a: Int, b: Int): Option[Int] = {
      if (b == 0) None else Some(a / b)
    }
    println(divideOption(10, 2)) // Some(5)
    println(divideOption(10, 0)) // None

    // Ejemplo de Try
    println("\nTry Monad:")
    def parseInt(s: String): Try[Int] = Try(s.toInt)
    println(parseInt("123")) // Success(123)
    println(parseInt("abc")) // Failure(java.lang.NumberFormatException)

    // Ejemplo de Future
    println("\nFuture Monad:")
    def fetchData(): Future[String] = Future {
      Thread.sleep(1000)
      "Datos obtenidos"
    }
    val futureResult = fetchData()
    println(Await.result(futureResult, 2.seconds)) // "Datos obtenidos"

    // Ejemplo de Either
    println("\nEither Monad:")
    def divideEither(a: Int, b: Int): Either[String, Int] = {
      if (b == 0) Left("División por cero") else Right(a / b)
    }
    println(divideEither(10, 2)) // Right(5)
    println(divideEither(10, 0)) // Left("División por cero")

    // Ejemplo de List
    println("\nList Monad:")
    val numbers = List(1, 2, 3, 4, 5)
    println(numbers.map(_ * 2)) // List(2, 4, 6, 8, 10)
    println(numbers.filter(_ % 2 == 0)) // List(2, 4)

    Thread.sleep(300000)
  }
}
