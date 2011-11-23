package net.debasishg.recon

import scalaz._
import Scalaz._
import scalaz.effects._
import IterV._

import java.io.{File, BufferedReader, Reader, FileReader}

object FileUtils {
  type Tagged[U] = { type Tag = U }
  type @@[T, U] = T with Tagged[U]

  trait Line
  trait Err

  type Record = String @@ Line
  type Error = String @@ Err

  def record(s: String): Record = s.asInstanceOf[Record]
  def err(s: String): Error = s.asInstanceOf[Error]

  def onException[A](e: Throwable)(f: Throwable => A) = IO(rw => (rw, f(e)))

  def bufferFile(f: File) = io {
    new BufferedReader(new FileReader(f))
  } 

  def closeReader(r: Reader) = io {
    r.close
  }

  def bracket[A,B,C](init: IO[A], fin: A => IO[B], body: A => IO[C]): IO[C] =
    for { 
      a <- init
      c <- body(a)
      _ <- fin(a) 
    } yield c

  def enumFile[A](f: File, i: IterV[String, A]): IO[IterV[String, A]] =
    bracket(bufferFile(f),
      closeReader(_:BufferedReader),
      enumReader(_:BufferedReader, i)) 

  def enumReader[A](r: BufferedReader, it: IterV[String, A]): IO[IterV[String, A]] = {
    def loop: IterV[String, A] => IO[IterV[String, A]] = {
      case i@Done(_, _) => io(i)
      case i@Cont(k) => for {
        s <- io { r.readLine }
        a <- if (s == null) io(i) else loop(k(El(s)))
      } yield a
    }
    loop(it)
  }

  val repeatHead = repeat[String, Option[String], List](head[String])
}
