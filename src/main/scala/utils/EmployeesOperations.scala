package es.azaharaclavero.sparkvideocourse
package utils

import Exercices.spark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import spark.implicits._

object EmployeesOperations {
  /**
   * Performs various operations on the employees DataFrame.
   *
   * @param employees The DataFrame containing employee data.
   */
  def employeesOperations (employees: DataFrame): Unit = {
    //Display all columns of the DataFrame, along with their respective data types
    employees.printSchema()

    //Create a temporary view named "employees" for the DataFrame
    employees.createOrReplaceTempView("employees")

    //SQL query to fetch solely the records from the View where the age exceeds 30
    val sqlQuery = spark.sql("SELECT * FROM employees WHERE Age > 30")
    sqlQuery.show()

    //SQL query to calculate the average salary of employees grouped by department

    val avgSalaryByDepartment = spark.sql("SELECT Department, AVG(salary) AS avgSalary from employees GROUP BY Department")
    println("Average Salary by Department:")
    avgSalaryByDepartment.show()

    //Apply a filter to select records where the department is 'IT'
    println("Dataframe filtrado por departamento IT")
    val filteredByIT = employees.filter($"Department" === "IT")
    filteredByIT.show()

    //Add a new column "SalaryAfterBonus" with 10% bonus added to the original salary
    println("Salary after bonus:")
    val salaryAfterBonus = employees.withColumn("salaryAfterBonus", round($"salary" * 1.10))
    salaryAfterBonus.show()

    //Group data by age and calculate the maximum salary for each age group
    println("Grouped by age with max salary:")
    val groupedByAge = employees.groupBy("age").agg(max("salary").alias("maxSalary"))
    groupedByAge.show()


    //Join the DataFrame with itself based on the "Emp_No" column
    println("Join the DataFrame with itself based on the Emp_No column:")
    val joinedDF = employees.join(employees, Seq("Emp_No"), "inner")
    joinedDF.show()

    //Calculate the average age of employees
    println("Average age of employees:")
    val avgAge = employees.agg(avg("age").alias("avgAge"))
    avgAge.show()
    //Calculate the total salary for each department. Hint - User GroupBy and Aggregate functions
    println("Total salary for each department:")
    val totalSalaryByDepartment = employees.groupBy("Department").agg(max("salary"))
    totalSalaryByDepartment.show()
    //Sort the DataFrame by age in ascending order and then by salary in descending order
    println("Order By age: ")
    val orderByAge = employees.orderBy($"age".asc)
    orderByAge.show()
    println("Order by salary: ")
    val orderBySalary = employees.orderBy($"salary".desc)
    orderBySalary.show()

    //Calculate the number of employees in each department
    println("Number of employees by department: ")
    val numberEmploByDepart = employees.groupBy(("Department")).agg(count("Emp_No").alias("numberOfEmployees"))
    numberEmploByDepart.show()

    //Apply a filter to select records where the employee's name contains the letter 'o'
    println("Filter by name containing 'o': ")
    val namesWithO = employees.filter($"Emp_name" contains("o"))
    namesWithO.show()
  }


}
