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
        var blocks = files.flatMap(f => BitcoinBlockParser.Parse(f))
        println("No of blocks is " + blocks.count())
        var blocksAsString = blocks.map(f => BitcoinBlockParser.Convert(f))

        blocksAsString.saveAsTextFile(args(1))

        session.close()

    }

    class Node{
        var id = ""
        var label = ""
        var graph = new Array[String](0)
        var props: Map[String, String] = Map()
    }

    class Edge{
        var id = ""
        var label = ""
        var src = ""
        var dst = ""
        var graph = new Array[String](0)
        val props = scala.collection.mutable.Map[String, String]()
    }

    object BitcoinBlockParser {
        var params = MainNetParams.get;
        var mapper = new ObjectMapper()
        mapper.registerModule(DefaultScalaModule)

        def Convert(f: Block): String = {
            var node = new Node()
            node.id = f.getHashAsString
            node.label = "block"
            node.props += ("PreviousBlockHash" -> f.getPrevBlockHash.toString)
            node.props += ("Nonce" -> f.getNonce.toString)
            node.props += ("Difficulty" -> f.getDifficultyTarget.toString)
            node.props += ("Version" -> f.getVersion.toString)
            node.props += ("MerkleRoot" -> f.getMerkleRoot.toString)
            node.props += ("Time" -> f.getTime.toString)

            return mapper.writeValueAsString(node)
        }


        def Parse(f: (String, PortableDataStream)): List[Block] = {

            //This line is required to init the context.
            val c = Context.getOrCreate(params)

            var blocks = List[Block]()

            val fileBytes = f._2.toArray()
            println("Size of the available block : " + fileBytes.length)

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
                blocks = blocks.::(block)
            }



            return  blocks
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
