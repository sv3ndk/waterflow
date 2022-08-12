package svend.playground.waterflow.http

import com.typesafe.scalalogging.Logger
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.{DefaultServlet, ServletContextHandler}
import org.eclipse.jetty.webapp.WebAppContext
import org.scalatra.LifeCycle
import org.scalatra.servlet.ScalatraListener

import java.util.concurrent.Executors
import javax.servlet.ServletContext
import scala.concurrent.ExecutionContext

object TaskHttpServer {

  val logger = Logger(classOf[TaskHttpServer.type])

  def startEmbeddedServer(port: Int = 8080): Server = {
    logger.info(s"Starting Jetty server on part $port")

    val server = new Server(port)
    val context = new WebAppContext()
    context setContextPath "/"
    context.setResourceBase("src/main/webapp")
    context.setInitParameter(ScalatraListener.LifeCycleKey, classOf[ScalatraBootstrap].getName)
    context.addServlet(classOf[DefaultServlet], "/")
    server.setHandler(context)
    server.start()
    server
  }
}

class ScalatraBootstrap extends LifeCycle {
  val javaExecutorService = Executors.newFixedThreadPool(2)
  val executionContext = ExecutionContext.fromExecutor(javaExecutorService)
  val logger = Logger(classOf[ScalatraBootstrap])

  override def init(context: ServletContext): Unit = {
    context.mount(TaskExecutorServlet(executionContext), "/*")
  }

  override def destroy(context: ServletContext): Unit = {
    logger.info("Shutting down Task Executor Execution context")
    javaExecutorService.shutdown()
  }
}
