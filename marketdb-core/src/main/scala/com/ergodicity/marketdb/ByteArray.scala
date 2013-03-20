package com.ergodicity.marketdb

import java.util
import java.util.Arrays
import org.hbase.async.Bytes
import scalaz.ImmutableArray
import scalaz.ImmutableArray._

object ByteArray {
  def apply(xs: Byte*) = {
    val array = new Array[Byte](xs.length)
    var i = 0
    for (x <- xs.iterator) { array(i) = x; i += 1 }
    new ByteArray(array)
  }

  def apply(char: Char) = new ByteArray(Array[Byte](char.toByte))

  def apply(int: Int) = new ByteArray(Bytes.fromInt(int))

  def apply(long: Long) = new ByteArray(Bytes.fromLong(long))

  def apply(array: Array[Byte]) = new ByteArray(array)
  
  def apply(str: String) = new ByteArray(str.getBytes)
  
  def apply(array: ImmutableArray[Byte]) = new ByteArray(array.toArray)
}

/**
 * Immutable byte array
 * @param array array of bytes to construct from
 */
class ByteArray(array: Array[Byte]) {
  val internal = ImmutableArray.fromArray(array)  
  
  override def equals(that: Any) = that match {
    case ba: ByteArray => util.Arrays.equals(toArray, ba.toArray)
    case _ => false
  }
  
  def toArray = internal.toArray

  def ++(other: ByteArray) = ByteArray(internal ++ other.internal)

  def slice(from: Int, until: Int) = ByteArray(internal.slice(from, until))

  def length = internal.length

  def isEmpty = internal.isEmpty

  def foldLeft[B](z: B)(op: (B, Byte) => B): B = internal.foldLeft(z)(op)

  def asString = new String(toArray)

  override def hashCode() = Arrays.hashCode(toArray)

  override def toString = "ByteArray"+Arrays.toString(toArray)
}
