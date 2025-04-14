package es.azaharaclavero.sparkvideocourse

package object utils {

  // Column names
  object EmployeesColumnNames  {
    val EmpNumber = "Emp_no"
    val EmpName = "Emp_name"
    val Salary = "Salary"
    val Age = "Age"
    val Department = "Department"
  }

  object AbcColumnNames  {
    val Date = "date"
    val Open = "Open"
    val High = "High"
    val Low = "Low"
    val Close = "Close"
    val Adj_Close = "Adj Close"
    val Volume = "Volume"
  }
}


// Un package object es una característica que permite definir miembros (como funciones, variables, tipos, etc.)
// que están disponibles en to-do el paquete. Esto es útil para compartir utilidades comunes entre varias clases y
// objetos dentro del mismo paquete

