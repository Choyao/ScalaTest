import java.io.File

class Rational(n: Int, d: Int) {
  require(d != 0)
  private val g = gcd(n.abs, d.abs)

  val num = n / g
  val denom = d / g

  def this(n: Int) = this(n, 1)

  override def toString = num + "/" + denom

  def add(that: Rational): Rational = {
    new Rational(denom * that.num + num * that.denom, d * that.denom)
  }

  def +(that: Rational): Rational =
    new Rational(num * that.denom + denom * that.num, denom * that.denom)

  def +(i: Int): Rational =
    new Rational(num + i * denom, denom)

  def -(that: Rational): Rational =
    new Rational(num * that.denom - that.num * denom, denom * that.denom)

  def -(i: Int): Rational =
    new Rational(num - i * denom, denom)

  def *(that: Rational): Rational =
    new Rational(num * that.num, denom * that.denom)

  def *(i: Int): Rational =
    new Rational(num * i, denom)

  def /(that: Rational): Rational =
    new Rational(num * that.denom, denom * that.num)

  def /(i: Int): Rational =
    new Rational(num, denom * i)

  private def gcd(n: Int, d: Int): Int =
    if (d == 0) n else gcd(d, n % d)


}


object Rational {



  def main(args: Array[String]): Unit = {
    print(multTable())


  }

  def serchFile(files : Array[File]):Int ={
    1
  }
  def makerRowSeq(row: Int)  = {
    for(col <- 1 to 10) yield {
      val prod = (row * col).toString
      val padding = " " * (4 - prod.length)
      padding + prod
    }
  }

  def makeRow(row: Int) = makerRowSeq(row).mkString

  def multTable() = {
    val tableSqu = {
      for (row <- 1 to 10) yield makeRow(row)
    }
    tableSqu.mkString("\n")
  }


  /*def makerRowSeq(row: Int)  = {
    for(col <- 1 to row) yield {
      val line = (row * col).toString
      val res = row  +"*" + col  + "="  + line
      res
    }
  }

  def makeRow(row: Int) = makerRowSeq(row).mkString

  def multTable() = {
    val tableSqu ={
      for(row <- 1 to 10) yield makeRow(row)
    }
    tableSqu.mkString("\n")
  }*/


  def fileLines(file: java.io.File) = scala.io.Source.fromFile(file).getLines().toList

  def gcdLoop(a: Long, b: Long): Long = {
    var x = a //12
    var y = b //18
    while ((x != 0)) {
      val temp = x //12  6
      x = y % x //18 % 12 == 6
      y = temp //12
    }
    y
  }
}


