package net.debasishg.recon

import scalaz._
import Scalaz._
import scalaz.effects._
import IterV._

import java.io.{File, BufferedReader, Reader, FileReader}

object FileUtils {

  def onException[A](e: Throwable)(f: Throwable => A) = IO(rw => (rw, f(e)))

  def bufferFile(f: File) = io {
    new BufferedReader(new FileReader(f), 20000)
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

  /**
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
  **/

  def enumReader[A](r: BufferedReader, it: IterV[String, A]): IO[IterV[String, A]] = { 
    def loop(x: IterV[String, A]): IterV[String, A] = x match { 
      case Done(_, _) => x 
      case Cont(k) => { 
        val s = r.readLine 
        if (s == null) x else loop(k(El(s))) 
      } 
    } 
    io { loop(it) } 
  } 


  val repeatHead = repeat[String, Option[String], List](head[String])
}
