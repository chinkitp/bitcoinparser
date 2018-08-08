package com.chinkit.bitcoinparser

import org.apache.spark.input.PortableDataStream
import org.apache.spark.sql.SparkSession
import org.bitcoinj.core.{Block, Context, Utils}
import org.bitcoinj.params.MainNetParams

object App {

    def main(args: Array[String]): Unit = {

        val session = SparkSession.builder()
            .appName(getClass.getSimpleName)
            .master("local[*]")
            .getOrCreate()

        val files = session.sparkContext.binaryFiles("/Users/chinkit/Downloads/blk*.dat")
        var blocks = files.flatMap(f => MyFunctions.func2(f))


        println(blocks.count())

        session.close()

    }

    object MyFunctions {
        var params = MainNetParams.get;
        def func2(f: (String, PortableDataStream)): List[Block] = {

            val c = Context.getOrCreate(params)
            var blocks = List[Block]()

            var stream = f._2.open()
            val fileBytes = new Array[Byte](stream.available())
            stream.readFully(fileBytes)
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
