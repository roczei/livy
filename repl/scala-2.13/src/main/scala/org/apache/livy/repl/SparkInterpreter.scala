/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.livy.repl

import java.io.{File, PrintWriter}
import java.net.{URL, URLClassLoader}
import java.nio.file.{Files, Paths}

import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.{IMain, Repl}
import scala.tools.nsc.interpreter.Results.Result
import scala.tools.nsc.interpreter.shell.{Completion, NoCompletion, ReplCompletion}

import org.apache.spark.SparkConf
import org.apache.spark.repl.SparkILoop

/**
 * Spark 4.x / Scala 2.13 implementation of the Spark interpreter.
 *
 * The Spark 4 SparkILoop is built on top of the rewritten
 * `scala.tools.nsc.interpreter.shell.ILoop` API. It exposes an `intp`
 * (Repl / IMain) through which most bind/interpret/quiet operations flow;
 * the old `beQuietDuring` / `lastRequest` accessors that used to live on
 * SparkILoop itself in Scala 2.12 now live on `intp`.
 *
 * This class is not thread safe.
 */
class SparkInterpreter(protected override val conf: SparkConf) extends AbstractSparkInterpreter {

  private var sparkILoop: SparkILoop = _

  override def start(): Unit = {
    require(sparkILoop == null)

    val rootDir = conf.get("spark.repl.classdir", System.getProperty("java.io.tmpdir"))
    val outputDir = Files.createTempDirectory(Paths.get(rootDir), "spark").toFile
    outputDir.deleteOnExit()
    conf.set("spark.repl.class.outputDir", outputDir.getAbsolutePath)

    val settings = new Settings()
    settings.processArguments(List("-Yrepl-class-based",
      "-Yrepl-outdir", s"${outputDir.getAbsolutePath}"), true)
    settings.usejavacp.value = true
    settings.embeddedDefaults(Thread.currentThread().getContextClassLoader())

    // Spark 4's SparkILoop takes (BufferedReader, PrintWriter). A null reader
    // is fine -- we never drive input through the loop; we only use its intp.
    sparkILoop = new SparkILoop(null, new PrintWriter(outputStream, true))

    // Scala 2.13's shell.ILoop constructs the interpreter via
    // `createInterpreter(settings)`. There is no post-construction `settings=`
    // setter -- settings are passed in here.
    sparkILoop.createInterpreter(settings)

    restoreContextClassLoader {
      var classLoader = Thread.currentThread().getContextClassLoader
      while (classLoader != null) {
        if (classLoader.getClass.getCanonicalName ==
          "org.apache.spark.util.MutableURLClassLoader") {
          val extraJarPath = classLoader.asInstanceOf[URLClassLoader].getURLs()
            // Check if the file exists. Otherwise an exception will be thrown.
            .filter { u => u.getProtocol == "file" && new File(u.getPath).isFile }
            // Livy rsc and repl are also in the extra jars list. Filter them out.
            .filterNot { u => Paths.get(u.toURI).getFileName.toString.startsWith("livy-") }
            // Some bad spark packages depend on the wrong version of scala-reflect. Blacklist it.
            .filterNot { u =>
              Paths.get(u.toURI).getFileName.toString.contains("org.scala-lang_scala-reflect")
            }

          extraJarPath.foreach { p => debug(s"Adding $p to Scala interpreter's class path...") }
          // addUrlsToClassPath lives on the Repl (IMain) in 2.13, not on ILoop.
          // The IMain.classLoader (URLClassLoader) is initialised lazily by the
          // Scala compiler on first use and may still be null at this point.
          // Swallow the resulting NPE; the URLs are already on the Spark
          // MutableURLClassLoader we found above, so they are accessible to
          // the interpreter anyway.
          try {
            if (sparkILoop.intp != null) {
              sparkILoop.intp.addUrlsToClassPath(extraJarPath: _*)
            }
          } catch {
            case _: NullPointerException =>
              debug("IMain URLClassLoader not yet ready; skipping addUrlsToClassPath " +
                "(URLs are already on the MutableURLClassLoader)")
          }
          classLoader = null
        } else {
          classLoader = classLoader.getParent
        }
      }

      postStart()
    }
  }

  override def close(): Unit = synchronized {
    super.close()

    if (sparkILoop != null) {
      sparkILoop.closeInterpreter()
      sparkILoop = null
    }
  }

  override def addJar(jar: String): Unit = {
    sparkILoop.intp.addUrlsToClassPath(new URL(jar))
  }

  override protected def isStarted(): Boolean = {
    sparkILoop != null
  }

  override protected def interpret(code: String): Result = {
    sparkILoop.intp.interpret(code)
  }

  override protected def completeCandidates(code: String, cursor: Int) : Array[String] = {
    // Scala 2.13 replaced `PresentationCompilerCompleter` with
    // `shell.ReplCompletion`, which takes a `Repl` (the interface `IMain`
    // now implements). Instantiate it directly rather than by reflection.
    val completer: Completion =
      try new ReplCompletion(sparkILoop.intp.asInstanceOf[Repl])
      catch { case _: Throwable => NoCompletion }
    // CompletionCandidate exposes `name` in 2.13 (the older `defString` field
    // no longer exists on the case class).
    completer.complete(code, cursor, filter = false).candidates.map(_.name).toArray
  }

  override protected def valueOfTerm(name: String): Option[Any] = {
    // IMain#valueOfTerm will always return None, so reach into the last
    // request's line-representation directly.
    Option(sparkILoop.intp.asInstanceOf[IMain].lastRequest.lineRep.call("$result"))
  }

  override protected def bind(name: String,
      tpe: String,
      value: Object,
      modifier: List[String]): Unit = {
    // 2.13's shell.ILoop dropped `beQuietDuring`; suppress result printing
    // by calling through the reporter directly instead.
    sparkILoop.intp.reporter.withoutPrintingResults {
      sparkILoop.intp.bind(name, tpe, value, modifier)
    }
  }
}
