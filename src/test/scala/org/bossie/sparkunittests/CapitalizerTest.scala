package org.bossie.sparkunittests

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test
import org.junit.Assert._
import org.junit.runner.RunWith
import org.mockito.{ArgumentCaptor, Mock}
import org.mockito.junit.MockitoJUnitRunner
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import org.hamcrest.CoreMatchers.is

object CapitalizerTest {
  private val conf = new SparkConf().setMaster("local[*]").setAppName(classOf[CapitalizerTest].getName)
  private val sc = new SparkContext(conf)
}

@RunWith(classOf[MockitoJUnitRunner])
class CapitalizerTest {

  @Mock
  private var inputTextReader: String => RDD[String] = _

  @Mock
  private var outputTextWriter: RDD[String] => Unit = _

  @Test
  def testSingleLine(): Unit = {
    val inputLines = Seq("line 1")
    when(inputTextReader(anyString)).thenReturn(CapitalizerTest.sc.parallelize(inputLines))

    val capitalizer = new Capitalizer(inputTextReader, outputTextWriter)
    capitalizer.capitalize("/path/to/file");

    val argument: ArgumentCaptor[RDD[String]] = ArgumentCaptor.forClass(classOf[RDD[String]])
    verify(outputTextWriter)(argument.capture)

    val outputLines: Array[String] = argument.getValue.collect

    assertThat(outputLines, is(Array("LINE 1")))
  }

  @Test
  def testMultipleLines(): Unit = {
    val lineNumbers = 1 to 10000
    val inputLines = lineNumbers.map("line " + _)

    when(inputTextReader(anyString)).thenReturn(CapitalizerTest.sc.parallelize(inputLines, 10))

    val capitalizer = new Capitalizer(inputTextReader, outputTextWriter)
    capitalizer.capitalize("/path/to/file")

    val argument: ArgumentCaptor[RDD[String]] = ArgumentCaptor.forClass(classOf[RDD[String]])
    verify(outputTextWriter)(argument.capture)

    val actualOutputLines: Array[String] = argument.getValue.collect
    val expectedOutputLines= lineNumbers.map("LINE " + _).toArray

    assertThat(actualOutputLines, is(expectedOutputLines))
  }
}
