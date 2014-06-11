package scalding.examples

import com.twitter.scalding._

class WordCountJob(args : Args) extends Job(args) {
  //val extractor = """(http|https)://([^.]+[w|1]\.|)([^.]+\.[^/]+)/|.+""".r

  /*TextLine( args("input") )
    .flatMap('line -> 'word) {
      line : String => tokenize(line)
    }
    .groupBy('word) { _.size }
    .write( Tsv( args("output") ) )*/

  TextLine( args("input") )
    .map('line -> 'word) {line : String => {
    /*extractor.findAllIn(line).matchData.toList(0).group(3)}*/
    val host = new java.net.URL(line).getHost
    if (host.startsWith("www"))
      host.substring(host.indexOf('.')+1)
    else host
    }
  }
    .groupBy('word) { gp: GroupBuilder => gp.size }
    .filter('size){size: Int => size >= 5}
    .write(Tsv(args("output")))

  // split a piece of text into individual words
  /*def tokenize(text : String) : Array[String] = {
    text.toLowerCase.split(" ")
  }*/
}

object WordCountJob extends App {
  val progargs: Array[String] = List(
    "-Dmapred.map.tasks=200",
    "scalding.examples.WordCountJob",
    "--input", "/home/india/Desktop/Indix/scalding_examples/src/main/resources/sampleurls.txt",
    "--output", "/home/india/Desktop/Indix/scalding_examples/src/main/resources/output",
    "--hdfs"
  ).toArray
  Tool.main(progargs)
}
