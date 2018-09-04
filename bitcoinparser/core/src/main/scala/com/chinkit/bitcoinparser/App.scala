package com.chinkit.bitcoinparser

import java.io.StringWriter

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.input.PortableDataStream
import org.apache.spark.sql.SparkSession
import org.bitcoinj.core.{Block, Context, Utils}
import org.bitcoinj.params.MainNetParams
import org.json4s.jackson.Json
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

object App {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf
        conf.setIfMissing("spark.master", "local[*]")
        conf.setIfMissing("spark.app.name", getClass.getSimpleName)

        val session = SparkSession.builder()
            .config(conf)
            .getOrCreate()

        val files = session.sparkContext.binaryFiles(args(0))
        println("No of files is " + files.count())
        var blocks: RDD[Try[Block]] = files.flatMap(f => BitcoinBlockParser.Parse(f))

        val successBlocks: RDD[Block] = blocks.flatMap {
          case Success(x) => Some(x)
          case Failure(_) => None
        }

        val failureBlocks: RDD[BadBlockException] = blocks.flatMap {
           case Success(x) => None
           case Failure(t: BadBlockException) => Some(t)
       }

//        blocks.filter {
//          case Success(x) => true
//          case Failure(_) => false
//        } map (x => x.asInstanceOf[Success].value)


        //println("No of blocks is " + successBlocks.count())
        var blocksAsString = successBlocks.map(f => BitcoinBlockParser.Convert(f))

        //blocksAsString.collect().foreach(s=> printf(s))
        blocksAsString.saveAsTextFile(args(1))
        failureBlocks.map(b => (b.filename,b.message)).saveAsTextFile(args(2))

        session.close()

    }

    class Node(
      val id: String,
      val label: String,
      val graph: Array[String],
      val props: Map[String, AnyRef]
    )

/*    class Edge{
        var id = ""
        var label = ""
        var src = ""
        var dst = ""
        var graph = new Array[String](0)
        val props = scala.collection.mutable.Map[String, String]()
    }*/

    object BitcoinBlockParser {
        var params = MainNetParams.get;
        var mapper = new ObjectMapper()
        mapper.registerModule(DefaultScalaModule)

        def Convert(f: Block): String = {
            val node = new Node(
                f.getHashAsString,
                "block",
                Array.empty,
                Map(
                    "PreviousBlockHash" -> f.getPrevBlockHash.toString,
                    "Nonce" -> f.getNonce.toString,
                    "Difficulty" -> f.getDifficultyTarget.toString,
                    "Version" -> f.getVersion.toString,
                    "MerkleRoot" -> f.getMerkleRoot.toString,
                    "Time" -> f.getTime.toString
                )
            )

            return mapper.writeValueAsString(node)
        }


        def Parse(f: (String, PortableDataStream)): Seq[Try[Block]] = {

            //This line is required to init the context.
            val c = Context.getOrCreate(params)

            var blocks = Seq.newBuilder[Try[Block]]

            try
            {
                if(f._1.endsWith("blk00003.dat")){
                    throw new Exception("I don't like this file")
                }

                val fileBytes = f._2.toArray()
                //println("Size of the available block : " + fileBytes.length)

                //stream.readFully(fileBytes)
                var byteCursor = 0
                var blockCount = 0

                while (IsMagic(fileBytes, byteCursor)) {

                    blockCount = blockCount + 1;

                    var blockSizeBytes = new Array[Byte](4)
                    System.arraycopy(fileBytes,byteCursor + 4,blockSizeBytes,0,4)
                    val blockSize = Utils.readUint32BE(Utils.reverseBytes(blockSizeBytes), 0)
                    var blockBytes = new Array[Byte](blockSize.toInt)
                    System.arraycopy(fileBytes,byteCursor + 8,blockBytes,0, blockSize.toInt);
                    var block = params.getDefaultSerializer.makeBlock(blockBytes);
                    byteCursor = byteCursor + 8 + blockSize.toInt;
                    blocks += Success(block)
                }
            }
            catch {
                case e: Exception =>
                    blocks += Failure(new BadBlockException(f._1,e.getMessage))
            }

            return  blocks.result()
        }

        private def IsMagic(bytes: Array[Byte], startSeq: Int): Boolean= {
            if ((bytes.size != startSeq)
                && (bytes(startSeq) & 0xFF) == 0xF9
                && (bytes(startSeq + 1) & 0xFF) == 0xbe
                && (bytes(startSeq + 2) & 0xFF) == 0xb4
                && (bytes(startSeq + 3) & 0xFF) == 0xd9)
                return true

            false
        }

    }

}

case class BadBlockException(filename: String, message: String) extends Exception(message)
