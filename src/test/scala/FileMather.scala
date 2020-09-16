package scala

import java.io.{File, PrintWriter}

object FileMather {

  def main(args: Array[String]): Unit = {


    myAssert(5 / 0 == 0)

  }


  val istrue = false


  def myAssert(pridict:Boolean,x:Int)=
    if(istrue && !pridict)
      throw new AbstractMethodError

  def myAssert(pridict: => Boolean) =
    if (!pridict)
      throw new AssertionError

/*  def myAssert(prdicte: () => Boolean) = {
    if (istrue && !prdicte())
      throw new AssertionError
  }*/


  def withPrintWriter(file: File, op: PrintWriter => Unit) = {
    val writer = new PrintWriter(file)
    try {
      op(writer)
    } finally {
      writer.close()
    }
  }

  def curriesSum(x: Int)(y: Int) = x + y


  //以下是高阶函数
  def containsNeg(num: List[Int]) = num.exists(_ < 0)


  private def filesHere = (new java.io.File("./src/test/scala")).listFiles

  def fileEndings(query: String) = {
    for (file <- filesHere; if file.getName.endsWith(query))
      yield file
  }

  def fileMathing(mather: (String) => Boolean) = {
    for (file <- filesHere; if mather(file.getName))
      yield file
  }

  def fileEnding(query: String) = fileMathing(_.endsWith(query))

  def fileContaining(query: String) = fileMathing(_.contains(query))

  def fileRegex(query: String) = fileMathing(_.matches(query))


}
