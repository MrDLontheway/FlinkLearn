package com.wxstc.dl

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

object WordCount {
  def main(args: Array[String]): Unit = {
    //获取env 环境 也就是flink 环境信息 类似于spark context
    val env = ExecutionEnvironment.getExecutionEnvironment

    val outputPath = "D:\\_vm\\finkoutput"
    val text = env.readTextFile("D:\\_vm\\finkinput\\123.txt")

    val counts = text.flatMap { _.toLowerCase.split(",") filter { _.nonEmpty } }
      .map { (_, 1) }
      .groupBy(0)
      .sum(1)

    counts.writeAsCsv(outputPath, "\n", " ")

    //一定要加上这句  代表执行job  类似于 spark start
    env.execute("Scala WordCount Example")
    counts.print()
  }
}
