package com.chinkit.bitcoinparser

import java.io.StringWriter
import java.util

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.input.PortableDataStream
import org.apache.spark.sql.SparkSession
import org.bitcoinj.core.{Block, Context, Transaction, Utils}
import org.bitcoinj.params.MainNetParams
import org.json4s.jackson.Json
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Concat
import org.bitcoinj.wallet.Protos.TransactionInput

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

        val files: RDD[(String, PortableDataStream)] = session.sparkContext.binaryFiles(args(0))

        var parsedFiles: RDD[(String, Seq[String])] = files.map(f => BitcoinBlockParser.FileParse(f))

        parsedFiles.flatMap(f => f._2)
            .saveAsTextFile(args(2))


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

        def FileParse(f: (String, PortableDataStream)): (String, Seq[String]) = {

            val blocks: Seq[Try[Block]] = BitcoinBlockParser.Parse(f._1, f._2)

            val successBlocks: Seq[String] = blocks.filter(b => b.isSuccess)
                .map(b => Convert(b.get))
                .flatten(b => b)


            val failedBlocks: Seq[String] = blocks.filter(b => b.isFailure)
                .map(b => {
                    var exp = b.asInstanceOf[Failure[BadBlockException]].exception
                            .asInstanceOf[BadBlockException]
                    Seq[String](exp.filename,exp.message)
                })
                .flatten(b => b)

            (f._1,Seq concat(successBlocks, failedBlocks,Array("")))

        }

        def Convert(f: Block): Seq[String] = {

            try{
                val block = new Node(
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

                var transactions = new Array[App.Node](0)
                if(f.getTransactions != null && f.getTransactions.size() > 0){
                    transactions = f.getTransactions.toArray(new Array[Transaction](f.getTransactions.size()))
                        .map(t => new Node(
                            t.getHashAsString,
                            label = "transaction",
                            Array.empty,
                            Map(
                                "isCoinBase" -> t.isCoinBase.toString,
                                "isTimeLocked" -> t.isTimeLocked.toString,
                                "memo" -> t.getMemo,
                                "version" -> t.getVersion.toString
                            )
                        ))
                }

                return (Seq concat(transactions, Array(block)))
                    .map(b => mapper.writeValueAsString(b))

            }
            catch{
                case e: Exception =>
                    println(e.getMessage)
            }

            Array("")
        }


        def Parse(f: (String, PortableDataStream)): Seq[Try[Block]] = {

            //This line is required to init the context.
            val c = Context.getOrCreate(params)

            var blocks = Seq.newBuilder[Try[Block]]

            try
            {
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
