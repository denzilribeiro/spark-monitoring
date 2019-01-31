package org.apache.spark.metrics.microsoft.practices

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.codahale.metrics.MetricRegistry
import org.apache.spark.{SecurityManager, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.metrics.sink.Sink

private[spark] object LogAnalyticsSinkConfiguration {
  private[spark] val LOGANALYTICS_KEY_WORKSPACEID = "workspaceId"
  private[spark] val LOGANALYTICS_KEY_SECRET = "secret"
  private[spark] val LOGANALYTICS_KEY_LOGTYPE = "logType"
  private[spark] val LOGANALYTICS_KEY_TIMESTAMPFIELD = "timestampField"
  private[spark] val LOGANALYTICS_KEY_PERIOD = "period"
  private[spark] val LOGANALYTICS_KEY_UNIT = "unit"

  private[spark] val LOGANALYTICS_DEFAULT_LOGTYPE = "SparkMetrics"
  private[spark] val LOGANALYTICS_DEFAULT_PERIOD = "10"
  private[spark] val LOGANALYTICS_DEFAULT_UNIT = "SECONDS"
}

private[spark] class LogAnalyticsSinkConfiguration(properties: Properties)
  extends Logging {

  import LogAnalyticsSinkConfiguration._

  val workspaceId: String = {
    Option(properties.getProperty(LOGANALYTICS_KEY_WORKSPACEID)) match {
      case Some(value) => {
        logInfo(s"Setting workspaceId to ${value}")
        value
      }
      case None => throw new SparkException("A Log Analytics Workspace ID is required")
    }
  }

  val workspaceKey: String = {
    Option(properties.getProperty(LOGANALYTICS_KEY_SECRET)) match {
      case Some(value) => {
        logInfo(s"Setting workspaceId to ${value}")
        value
      }
      case None => throw new SparkException(s"A Log Analytics Workspace Key is required")
    }
  }

  val logType: String = properties.getProperty(LOGANALYTICS_KEY_LOGTYPE, LOGANALYTICS_DEFAULT_LOGTYPE)

  val timestampFieldName: Option[String] = Option(properties.getProperty(LOGANALYTICS_KEY_TIMESTAMPFIELD, null))

  val pollPeriod: Int = {
    val value = properties.getProperty(LOGANALYTICS_KEY_PERIOD, LOGANALYTICS_DEFAULT_PERIOD).toInt
    logInfo(s"Setting polling period to $value")
    value
  }

  val pollUnit: TimeUnit = {
    val value = TimeUnit.valueOf(
      properties.getProperty(LOGANALYTICS_KEY_UNIT, LOGANALYTICS_DEFAULT_UNIT).toUpperCase)
    logInfo(s"Setting polling unit to $value")
    value
  }
}

private class LogAnalyticsSink(
                                val property: Properties,
                                val registry: MetricRegistry,
                                securityMgr: SecurityManager)
  extends Sink with Logging {

  private val config = new LogAnalyticsSinkConfiguration(property)

  org.apache.spark.metrics.MetricsSystem.checkMinimalPollingPeriod(config.pollUnit, config.pollPeriod)

  var reporter = LogAnalyticsReporter.forRegistry(registry)
    .withWorkspaceId(config.workspaceId)
    .withWorkspaceKey(config.workspaceKey)
    .withLogType(config.logType)
    .build

  override def start(): Unit = {
    reporter.start(config.pollPeriod, config.pollUnit)
    logInfo(s"LogAnalyticsSink started with workspaceId: '${config.workspaceId}'")
  }

  override def stop(): Unit = {
    reporter.stop()
    logInfo("LogAnalyticsSink stopped.")
  }

  override def report(): Unit = reporter.report()
}
