package net.debasishg.recon

import scalaz._
import Scalaz._
import scalaz.effects._
import IterV._

import java.io.{File, BufferedReader, Reader, FileReader}

object FileUtils {
  def printStackTrace(e: Throwable) = IO(rw => (rw, e.printStackTrace))

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
