package com.middil.spark.test

import org.scalactic.Accumulation
import org.scalactic.Bad
import org.scalactic.Every
import org.scalactic.Good
import org.scalactic.Many
import org.scalactic.One
import org.scalactic.Or

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import org.specs2.mutable.Specification

class ProblemSpec extends Specification {

  val sc = new SparkContext("local", "test")

  val students = Seq(
    "Olivia",
    "Muhammad",
    "Santiago",
    "Aadhya",
    "Jing",
    "Maya",
    "Lucas",
    "Maria")

  val competencies = Seq(
    "numerals",
    "correspondence",
    "order",
    "cardinality",
    "abstraction",
    "subitizing",
    "quantity",
    "compaison",
    "symbols",
    "operations",
    "addition")

  val problems =
    Map(
      "1<2" -> Seq("numerals", "symbols", "cardinality", "comparison"),
      "1+1" -> Seq("numerals", "symbols", "quantity", "addition"),
      "1+2>2" -> Seq("numerals", "symbols", "quantity", "addition", "comparison"))

  val problemId = problems.keys.zipWithIndex.toMap

  val studentSubmissions = Seq(
    "Olivia" -> Seq(
      "1<2" -> "wrong",
      "1+1" -> "correct",
      "1+2>2" -> "correct"),
    "Muhammad" -> Seq(
      "1+1" -> "correct",
      "1<2" -> "correct",
      "1+2>2" -> "correct"),
    "Santiago" -> Seq(
      "1<2" -> "correct",
      "1+2>2" -> "wrong",
      "1+1" -> "correct"),
    "Aadhya" -> Seq(
      "1+1" -> "correct",
      "1+2>2" -> "wrong",
      "1<2" -> "correct"),
    "Jing" -> Seq(
      "1<2" -> "correct",
      "1+1" -> "correct",
      "1+2>2" -> "correct"),
    "Maya" -> Seq(
      "1<2" -> "correct",
      "1+1" -> "correct",
      "1+2>2" -> "correct"),
    "Lucas" -> Seq(
      "1+2>2" -> "wrong",
      "1<2" -> "correct",
      "1+1" -> "correct"),
    "Maria" -> Seq(
      "1+2>2" -> "wrong",
      "1+1" -> "correct",
      "1<2" -> "correct"))

  def validateStudent(student: String): Or[String, One[String]] =
    if (!students.contains(student))
      Bad(One(s"Student doesn't exist: $student"))
    else
      Good(student)

  def validateProblem(problem: String): Or[String, One[String]] =
    if (!problems.contains(problem))
      Bad(One(s"Problem doesn't exist: $problem"))
    else
      Good(problem)

  def validateResult(result: String): Or[String, One[String]] =
    if (!Seq("correct", "wrong").contains(result))
      Bad(One(s"Result doesn't exist: $result"))
    else
      Good(result)

  def validate(student: String, problem: String, result: String): Or[(String, String, String), Every[String]] = {
    Accumulation.withGood(validateStudent(student), validateProblem(problem), validateResult(result)) {
      (_, _, _)
    }
  }

  case class StudentResult(
    studentId: Long,
    student: String,
    problem: String,
    wasCorrect: Boolean)

  case class StudentTransition(
    wasCorrect: Boolean,
    size: Long,
    problem1: String,
    problem2: String)

  val results = for {
    ((student, submissions), studentId) <- studentSubmissions.zipWithIndex
    (problem, result) <- submissions
  } yield {
    StudentResult(studentId.toLong, student, problem, result == "correct")
  }

  "Student work" should {

    val transitions = for {
      ((p1, p2, b), results) <- results.zip(results.tail).groupBy(r => (r._1.problem, r._2.problem, r._1.wasCorrect))
    } yield {
      StudentTransition(b, results.size.toLong, p1, p2)
    }

    val vertices: RDD[(VertexId, String)] =
      sc.parallelize(problems.keys.zipWithIndex.map {
        case (p: String, i: Int) => (i.toLong, p)
      }.toSeq)

    val edges: RDD[Edge[(Boolean, Long)]] =
      sc.parallelize(transitions.map { transition =>
        Edge(problemId(transition.problem1).toLong,
          problemId(transition.problem2).toLong,
          (transition.wasCorrect, transition.size))
      }.toSeq)

    val defaultVertex = ("???")

    val graph = Graph(vertices, edges, defaultVertex)

    "give a graph" in {
      graph.vertices.collect must have size (3)
      graph.edges.collect must have size (12)
    }
  }
}
