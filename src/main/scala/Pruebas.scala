package es.azaharaclavero.sparkvideocourse


object Ejemplo {
  def main(args: Array[String]): Unit = {
    val mySaludo = new Saludos
    import mySaludo.saludo  // Importa el valor implícito de la clase
    mySaludo.imprimirSaludo()  // Imprimirá "Hola desde la clase!"
  }
}

class Saludos{
  implicit val saludo: String = "Hola desde la clase!"

  def imprimirSaludo()(implicit mensaje: String): Unit = {
    println(mensaje)
  }
}
