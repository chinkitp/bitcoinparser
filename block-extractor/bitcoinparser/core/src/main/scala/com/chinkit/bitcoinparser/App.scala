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
import org.apache.commons.codec.binary.Hex
import org.apache.hadoop.io.compress.GzipCodec
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

        val INPUT_DIR = args(0)
        val OUTPUT_DIR = args(1)
        val ERROR_DIR = args(2)

        val session = SparkSession.builder()
            .config(conf)
            .getOrCreate()

        var blocksAsArray: RDD[(String, Try[Seq[Array[Byte]]])] = session.sparkContext.binaryFiles(INPUT_DIR)
                .map(f => (f._1, Try(BitcoinStore.splitIntoBlocks(f._2.toArray()))))

        blocksAsArray.filter(f => f._2.isSuccess)
            .map(f =>  f._2.get)
            .flatMap(f => f)
            .map(f => Hex.encodeHexString(f))
            //.coalesce(50)
            .saveAsTextFile(OUTPUT_DIR,classOf[GzipCodec])

        blocksAsArray.filter(f => f._2.isFailure)
            .map(f =>  (f._1, f._2))
            .coalesce(1)
            .saveAsTextFile(ERROR_DIR)

        //        .map(a => Hex.encodeHexString(a))
        //        .saveAsTextFile(OUTPUT_DIR)

/*
        var blockStoreFileWithEpgmElements = session.sparkContext.binaryFiles(INPUT_DIR)
                .map(f => (f._1, Try(BitcoinStore.parse(f._2))))

        blockStoreFileWithEpgmElements.filter(f => f._2.isSuccess)
                .map(f => f._2.get)
                .flatMap(f => f)
                .coalesce(50)
                .saveAsTextFile(OUTPUT_DIR)

        blockStoreFileWithEpgmElements.filter(f => f._2.isFailure)
                .map(f =>  (f._1,f._2))
                .saveAsTextFile(ERROR_DIR)
*/
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

    object BitcoinStore {
        val params = MainNetParams.get;
        val c = Context.getOrCreate(params)

        var mapper = new ObjectMapper()
        mapper.registerModule(DefaultScalaModule)

        def parse(blockStoreFile: PortableDataStream) : Seq[String] = {
            splitIntoBlocks(blockStoreFile.toArray())
                .map(blockBytes => getBlock(blockBytes))
                .flatMap(block => getEpgmElements(block))
        }

        private def getEpgmElements(f: Block): Seq[String] = {

            val block = new Node(
                f.getHashAsString,
                "block",
                Array.empty,
                getBlockData(f)
            )

            val transactions = f.getTransactions.toArray(new Array[Transaction](f.getTransactions.size()))
                .map(t => new Node(
                    t.getHashAsString,
                    label = "transaction",
                    Array.empty,
                    getTransactionData(t)
                ))

            return (Seq concat(transactions, Array(block)))
                .map(b => mapper.writeValueAsString(b))
        }

        private def getTransactionData(t: Transaction) = {
            Map(
                "isCoinBase" -> t.isCoinBase.toString,
                "isTimeLocked" -> t.isTimeLocked.toString,
                "memo" -> t.getMemo,
                "version" -> t.getVersion.toString
            )
        }

        private def getBlockData(f: Block) = {
            Map(
                "PreviousBlockHash" -> f.getPrevBlockHash.toString,
                "Nonce" -> f.getNonce.toString,
                "Difficulty" -> f.getDifficultyTarget.toString,
                "Version" -> f.getVersion.toString,
                "MerkleRoot" -> f.getMerkleRoot.toString,
                "Time" -> f.getTime.toString
            )
        }

        def splitIntoBlocks(t: Array[Byte]) : Seq[Array[Byte]] = {
            var blocksAsBytes = Seq.newBuilder[Array[Byte]]
            var byteCursor = 0
            var blockCount = 0

            while (areNextBytesMagic(t, byteCursor)) {

                blockCount = blockCount + 1;

                var blockSizeBytes = new Array[Byte](4)
                System.arraycopy(t, byteCursor + 4, blockSizeBytes,0,4)
                val blockSize = Utils.readUint32BE(Utils.reverseBytes(blockSizeBytes), 0)
                var blockBytes = new Array[Byte](blockSize.toInt)
                System.arraycopy(t,byteCursor + 8,blockBytes,0, blockSize.toInt);
                byteCursor = byteCursor + 8 + blockSize.toInt;
                blocksAsBytes += blockBytes
            }

            blocksAsBytes.result()
        }

        private def getBlock(b: Array[Byte]) : Block = {
            params.getDefaultSerializer.makeBlock(b);
        }

        private def areNextBytesMagic(bytes: Array[Byte], startSeq: Int): Boolean= {
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

