package com.middil.spark

object Main extends App {
  for {
    arg <- args
  } yield {
    println(arg)
  }
}
