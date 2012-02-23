package com.ergodicity.marketdb.loader.util

import scalaz._
import Scalaz._
import IterV._
import scalaz.effects._
import java.io.{Reader, InputStreamReader, InputStream, BufferedReader}

object InputStreamIteratee {
  def enumReader[A](r: BufferedReader, it: IterV[String, A]): IO[IterV[String, A]] = {
    def loop: IterV[String, A] => IO[IterV[String, A]] = {
      case i@Done(_, _) => i.pure[IO]
      case i@Cont(k) => for {
        s <- r.readLine.pure[IO]
        a <- if (s == null) i.pure[IO] else loop(k(El(s)))
      } yield a
    }
    loop(it)
  }

  def bufferInputStream(is: InputStream) = new BufferedReader(new InputStreamReader(is)).pure[IO]

  def closeReader(r: Reader) = r.close().pure[IO]

  def bracket[A, B, C](init: IO[A], fin: A => IO[B], body: A => IO[C]): IO[C] =
    for {
      a <- init
      c <- body(a)
      _ <- fin(a)
    } yield c

  def enumInputStream[A](is: InputStream, i: IterV[String, A]): IO[IterV[String, A]] =
    bracket(bufferInputStream(is),
      closeReader(_: BufferedReader),
      enumReader(_: BufferedReader, i))
}

