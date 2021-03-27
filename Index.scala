package search.sol
import java.io._
import java.util.Arrays
import search.src.StopWords.isStopWord
import search.src.PorterStemmer.stemArray
import search.src.FileIO.{printDocumentFile, printTitleFile, printWordsFile}

import java.util
import scala.collection.{immutable, mutable}
import scala.xml.{Node, NodeSeq}
import scala.util.matching.Regex
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import mutable.{ArrayBuilder, ArraySeq}
import scala.math.{floor, pow, sqrt}

/**
 * Provides an XML indexer, produces files for a querier
 *
 * @param inputFile - the filename of the XML wiki to be indexed
 */
class Index(val inputFile: String) {
  // TODO : Implement!
  val mainNode:   Node = xml.XML.loadFile(inputFile)
  val pageSeq:    NodeSeq = mainNode \ "page"
  val titleSeq:   NodeSeq = mainNode \\ "title"
  val idSeq:      NodeSeq = mainNode \\ "id"
  var textSeq:    NodeSeq = mainNode \\ "text"

  val length: Int = pageSeq.length
  val regex = new Regex("""\[\[[^\[]+?\]\]|[^\W_]+'[^\W_]+|[^\W_]+""")

  //var idMap:    mutable.HashMap[Int, mutable.HashMap[String, Int]] = new mutable.HashMap()
  var ajMap:    mutable.HashMap[Int, Double] = new mutable.HashMap
  var rankMap:  mutable.HashMap[Int, Double] = new mutable.HashMap
  var wordMap:  mutable.HashMap[String, mutable.HashMap[Int, Double]] = new mutable.HashMap()

  var binaryCount = 0

  def generateWordMap(): (mutable.HashMap[String, mutable.HashMap[Int, Double]], mutable.HashMap[Int,Double]) ={

    System.out.println("Started processing text and building hashmap")
    val cleanCorpus = this.cleanLinks(stemArray(regex.findAllMatchIn(textSeq.text).toList.map{aMatch => aMatch.matched}.filter(word => !isStopWord(word)).toArray).map(word => word.toLowerCase.trim).toList).distinct.sorted //this is a list

    // populate outer hashmap (initialise)
    for (outerIndex <- cleanCorpus.indices) {
      wordMap += (cleanCorpus(outerIndex) -> new mutable.HashMap[Int, Double])
      if (outerIndex%25000 == 0) System.out.println("HashMap Builder: " + outerIndex)
    }
    System.out.println("Finished processing text and building map!" + "\n")

    for(id <- 0 until length) { //length

      val cleanPage = this.cleanLinks(stemArray(regex.findAllMatchIn(textSeq(id).text).toList.map { aMatch => aMatch.matched }.filter(word => !isStopWord(word)).toArray).map(word => word.toLowerCase).toList).sorted

      var pageIndex = 0
      var unconIndex = 0

      var aj = 0

      while (pageIndex < cleanPage.length) {

        val currentWord = cleanPage(pageIndex)
        var currentFreq = 0

        while (currentWord == cleanPage(pageIndex) & !(cleanPage(pageIndex) == cleanPage.last)) {
          currentFreq = currentFreq + 1
          if (pageIndex < cleanPage.length - 1) {
            pageIndex = pageIndex + 1
          }
          unconIndex = unconIndex + 1
        }

        wordMap(currentWord) += (id -> currentFreq)

        if (unconIndex == cleanPage.length - 1 | (cleanPage(pageIndex) == cleanPage.last)) {
          if (currentWord != cleanPage(pageIndex)) {
            val lastWord = cleanPage(pageIndex)
            currentFreq = 1
            wordMap(lastWord) += (id -> currentFreq)

          } else {
            currentFreq = cleanPage.length - pageIndex + 1
            wordMap(currentWord) += (id -> currentFreq)

          }
          pageIndex = pageIndex + 1 //EXIT CLAUSE
        }

        if (currentFreq > aj) aj = currentFreq

      }
      if (id%20 == 0) System.out.println("Word Map Tracker: " + id)
      ajMap += (id -> aj)
    }
    (wordMap, ajMap)
  }



  def getPageLinks(words: List[String]): List[String] = {
    val arrayWords: Array[String] = words.toArray
    val length: Int = words.length
    var linkList = new ListBuffer[String];

    for (w <- 0 until length) {
      if (arrayWords(w).startsWith("[[")) {
        linkList += arrayWords(w)
      }
    }
    linkList.toList
  }


  //TODO: remove redundant variables to optimise time/space efficiency
  // done!
  def cleanLinks(words: List[String]): List[String] = {

    val arrayWords: Array[String] = words.toArray
    val length: Int = words.length

    for (w <- 0 until length) {
      if (arrayWords(w).startsWith("[[")) {
        arrayWords(w) = arrayWords(w).substring(2, arrayWords(w).length - 2)
      }

      //TODO: edge case for length (found in bigwiki)
      if (arrayWords(w).startsWith("category:") & arrayWords(w).length > 9) {
        arrayWords(w) = arrayWords(w).substring(9, arrayWords(w).length)
      }

      if (arrayWords(w).contains("|")) {
        arrayWords(w) = arrayWords(w).substring(arrayWords(w).indexOf("|") + 1, arrayWords(w).length).trim
      }

      arrayWords(w).trim
    }
    arrayWords.toList
  }


  /**
   * write a `String` to the `filename`.
   */
  def titlesTxt(): Unit = {
    val file = new File("/Users/Advay/IdeaProjects/search-master/src/search/src/bigtitles.txt")
    val bw = new BufferedWriter(new FileWriter(file))
    for (i <- 0 until length) {
      bw.write(idSeq(i).text.trim + " " + titleSeq(i).text.trim + "\n")
    }
    bw.close()
  }


  //TODO: try different damping? WHAT IS HAPPENING?
  def pageRank(): mutable.HashMap[Int, Double] = {

    val initRank: Double = 1.00 / length

    System.out.println("init rank: " + initRank)

    val weightR: Array[Double] = this.weights(0.15) //can adjust eps
    val weightOuter = new Array[Array[Double]](length)
    val ranks = new Array[Double](length)

    for (e <- 0 until length) {
      ranks(e) = initRank
    }

    for (outer <- 0 until length) {
      val weightInner = new Array[Double](length)
      for (inner <- 0 until length) {
        weightInner(inner) = weightR((inner * length) + outer)
      }
      //System.out.println(weightInner.mkString("Array(", ", ", ")"))
      weightOuter(outer) = weightInner
    }

    var valsNotConverged: Boolean = true

    var count = 0

    while (valsNotConverged) {

      val tempRanks = new Array[Double](length)

      for (e <- 0 until length) {
        tempRanks(e) = ranks(e)
      }

      //TODO: see medwiki without correction term
      //System.out.println("total prob: " + ranks.toList.sum)

      //TODO: this might be kinda convoluted but-
      // can we pull out all the zeros and the len-1
      // store those in a seperate bucket
      // if they are factored out, they won't need extra iterations in dot prod?
      // actually idk sounds annoying to implement ngl
      for (e <- 0 until length) { //length
        ranks(e) = dotProduct(weightOuter(e), tempRanks)
        //System.out.println(idSeq(e).text.trim + " dot prod: " + ranks(e) + "\n")
      }

      //System.out.println("total prob: " + ranks.toList.sum)

      //TODO: lol yeah honestly that distance value is a pain
      if (ranks.toList.sum > 1.2) {
        //TODO: ideally this branch should not exist
        // using sum and if in loop -- quadratic time -- BAD!
        System.out.println("\n" + "Pls kill me :)")
        System.out.println("iterations before death: " + count)
        valsNotConverged = false
      } else {
        valsNotConverged = this.getDistance(ranks, tempRanks) > 0.00000000000001
      }

      count = count + 1
    }

    System.out.println("total prob: " + ranks.toList.sum)
    System.out.println("total iterations: " + count)

    //    System.out.println("total prob: " + ranks.toList.sum)
    //    System.out.println("ranks array: " + ranks.mkString("Array(", ", ", ")"))
    //    System.out.println("length: " + ranks.length + "\n")

    for (i <- 0 until length) {
      rankMap += (i -> ranks(i))
    }
    rankMap
  }

  //TODO: any ideas for optimization?
  def getDistance(newRank : Array[Double], oldRank: Array[Double]): Double ={
    var distance = 0.0
    for (i <- newRank.indices) {
      distance = distance + (newRank(i) - oldRank(i)) * (newRank(i) - oldRank(i))
    }
    sqrt(distance)
  }


  def dotProduct(weightA: Array[Double], oldRanks: Array[Double]): Double = {
    var product: Double = 0.0

    //TODO: there are many cases where all values in weight are the same (len-1)
    // consider checking for this outside, so we don't need to loop in this case
    // linear to constant!

    //TODO: the  correction factor is hacky af -- consider fixing code?
    //TODO: even if correction factor is a valid implementaion, slow af since extra linear element from sum
    val sum = oldRanks.toList.sum

    for (i <- oldRanks.indices) {
      product = product + weightA(i) * (oldRanks(i) / pow(sum,1.7) )
    }
    //System.out.println("final product: " + product + "\n")
    product
  }


  //TODO: this is a MASSIVE bottleneck when lenght is large!
  // many improvements necessary
  def weights(eps: Double): Array[Double] = {

    val weightArray = new Array[Double](length * length)
    util.Arrays.fill(weightArray, eps/length)
    val titleList = titleSeq.text.toLowerCase.trim.split("\n" + "\n").sorted

    //System.out.println(titleList.mkString("Array(", ", ", ")") + "\n")
    // j -> toPage
    // k -> fromPage

    //val file = new File("/Users/Advay/IdeaProjects/search-master/src/search/src/bignkvals.txt")
    //val bw = new BufferedWriter(new FileWriter(file))

    //TODO: SPEED NEEDED!
    for (fromPage <- 0 until length) {

      if (fromPage%50 == 0) System.out.println("weight progress: " + fromPage)

      //TODO: reduce redundancies in this repeated code
      val nkArray = new Array[Int](length)

      val matchesIterator = regex.findAllMatchIn(textSeq(fromPage).text)
      val matchesList = matchesIterator.toList.map { aMatch => aMatch.matched }

      val distinctWords = stemArray(matchesList.filter(word => !isStopWord(word)).toArray).map(word => word.toLowerCase).distinct

      val cleanedLinks = this.cleanLinks(this.getPageLinks(distinctWords.toList)).distinct

      var bufferLinks: ArrayBuffer[String] = this.toArrayBuffer(cleanedLinks)

      //System.out.println("Before Loop:" + bufferLinks.toString)

      var index = 0

      //TODO: try combining these loops? only iterate through once then?
      while (index < bufferLinks.length) {
        if (!titleList.contains(bufferLinks(index)) | titleList(fromPage) == bufferLinks(index)) {
          bufferLinks -= bufferLinks(index)
        } else {
          index = index + 1
        }
      }

      val bufferLength = bufferLinks.length

      if (bufferLength == 0) {
        util.Arrays.fill(nkArray, length - 1)
      } else {
        for (i <- 0 until bufferLength){
          nkArray(binarySearch(bufferLinks(i), titleList, 0, floor(length/2).toInt, length)) = bufferLength
        }
      }

      for (toPage <- 0 until length) {
        if (nkArray(toPage) != 0 & toPage != fromPage) weightArray(fromPage * length + toPage) = eps/length + (1-eps) / nkArray(toPage)
      }

      //      System.out.println(idSeq(fromPage).text.trim + " nk vals: " + nkArray.mkString("Array(", ", ", ")"))
      //      System.out.println(idSeq(fromPage).text.trim + " sum: " + nkArray.sum + "\n")
    }

    //bw.close()
    weightArray
  }


  //TODO: we need to find a way to initialise as a buffer, the conversion really gums up process
  def toArrayBuffer(lst: List[String]): ArrayBuffer[String] = {
    val buffer = new ArrayBuffer[String]()
    for (word <- lst.indices) {
      buffer += lst(word)
    }
    buffer
  }


  def binarySearch(target: String, array : Array[String], lower: Int, pivot: Int, higher: Int): Int = {

    binaryCount = binaryCount + 1
    //System.out.println("current word: " + array(pivot))

    if (array(pivot) == target) {
      //System.out.println("number of recursions: " + binaryCount)
      binaryCount = 0
      pivot
    } else if (target < array(pivot)) {
      binarySearch(target, array, lower, floor((pivot+lower)/2).toInt, pivot)
    } else {
      binarySearch(target, array, pivot, floor((higher+pivot)/2).toInt, higher)
    }
  }

}


object Index {
  def main(args: Array[String]){

    val index: Index = new Index(args(0))
    val now = System.currentTimeMillis()

    // Data for txt files:

    // Titles.txt
    var idsToTitles: mutable.HashMap[Int, String] = new mutable.HashMap()

    for (i <- 0 until index.length) {
      idsToTitles += (i -> index.titleSeq(i).text.trim)
    }

    printTitleFile(args(1), idsToTitles)
    System.out.println("titles done!" + "\n")

    //TODO: maybe some redundant methods here
    // reimplement to suit the requisite output

    val generateWordMapTuple = index.generateWordMap()

      printDocumentFile(args(2), generateWordMapTuple._2, index.pageRank())
    System.out.println("documents done!" + "\n")

    //printWordsFile
    printWordsFile(args(3), generateWordMapTuple._1)

    var timeElapsed: Double = System.currentTimeMillis() - now
    System.out.println("Runtime: " + timeElapsed/1000 + " seconds")

    //TODO: big wiki runtime for titles, docs = 4949 secs,     target runtime = 600 secs
    // TODO: med wiki runtime for titles, docs = 82 secs,       target runtime = 60 secs

    //TODO: since weight seems quadratic, or worse, focus on that first, then AJ vals
    // TODO: words will probably be the slowest


    //original runtime 1 to 20 = 100.872 seconds
    //new runtime 1 to 20 = 0.644 seconds
  }
}
