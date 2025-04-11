package es.azaharaclavero.sparkvideocourse

import com.typesafe.config.ConfigFactory
import es.azaharaclavero.sparkvideocourse.utils._
import org.apache.spark.sql.functions.{avg, col, count, max, round, sum}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


object Exercices extends SparkConfiguration with SparkUtils {
  trait DataFrameOperations {
    def showDataFrame(df: DataFrame): Unit = {
      df.show()
    }

    def countRows(df:DataFrame):Long = {
      df.count()
    }
  }
  object EmployeeOperations extends DataFrameOperations


  def main(args: Array[String]): Unit = {
   val config = ConfigFactory.load()

    // Generate a spark dataframe from a csv using the SparkUtils trait
    val employeesDF: DataFrame = readCVS(spark, path = config.getString("spark.employeesPath"))

    // Define a Schema for the input data and read the file using the user-defined Schema
    val employees = employeesDF
      .withColumn(EmployeesColumnNames.EmpNumber, col(EmployeesColumnNames.EmpNumber).cast(IntegerType))
      .withColumn(EmployeesColumnNames.EmpName, col(EmployeesColumnNames.EmpName).cast(StringType))
      .withColumn(EmployeesColumnNames.Salary, col(EmployeesColumnNames.Salary).cast(DoubleType))
      .withColumn(EmployeesColumnNames.Age, col(EmployeesColumnNames.Age).cast(IntegerType))
      .withColumn(EmployeesColumnNames.Department, col(EmployeesColumnNames.Department).cast(StringType))

    EmployeesOperations.employeesOperations(employees: DataFrame)

    EmployeeOperations.showDataFrame(employees)
    println(s"Total de filas: ${EmployeeOperations.countRows(employees)}")
    /**
     *    Create a function that takes an employee number and a DataFrame as input and returns the employee's details.
     *    If the employee is not found, return None.
     *    If multiple employees are found with the same number, throw an exception.
     */
    def findEmployeeById (emp_no: Int, employees: DataFrame): Option[Row] = {
      val result: List[Row] = employees.filter(col(EmployeesColumnNames.EmpNumber) === emp_no).collect().toList
      result.foreach(println)
      result match {
        case Nil => None
        case head :: Nil => Some(head)
        case _ => throw new Exception (s"Multiple employees found with same Emp_No: $EmployeesColumnNames.EmpNumber")
      }
    }

    val employeeOption = findEmployeeById(12, employees)
    employeeOption match {
      case Some(employee) => println(s"Empleado encontrado: $employee")
      case None => println("Empleado no encontrado")
    }
  }
}
