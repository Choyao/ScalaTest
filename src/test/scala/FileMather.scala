package scala

object FileMather {




  private def filesHere = (new java.io.File(".")).listFiles

  def fileEndings(query : String) = {
    for(file <- filesHere;if file.getName.endsWith(query))
      yield file
  }

  def fileMathing(query:String ,mather:(String,String) => Boolean) = {
    for(file <- filesHere;if mather(file.getName,query))
      yield file
  }

  def fileEnding(query:String) = fileMathing(query,_.endsWith(_))

  def fileContaining(query:String) = fileMathing(query,_.contains(_))

  def fileRegex(query:String) = fileMathing(query,_.matches(_))















}
