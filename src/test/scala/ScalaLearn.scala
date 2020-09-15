import scala.io.Source

class ScalaLearn {

}


object ScalaLearn {

  def main(args: Array[String]): Unit = {
    processFile("./src/test/scala/ScalaLearn.scala",45)
  }

  def processFile(fileName: String, width: Int) = {

    def processLines(line: String) = {

      if(line.length > width)
        print(fileName + ":" + line)

    }

    val source = Source.fromFile(fileName)
    for (line <- source.getLines()) {
      processLines(line)
    }
  }
//
//  private def processLines(fileName: String, line: String, width: Int) = {
//    if (line.length > width) {
//      println(fileName + ":" + line.trim)
//    }

//  }
}