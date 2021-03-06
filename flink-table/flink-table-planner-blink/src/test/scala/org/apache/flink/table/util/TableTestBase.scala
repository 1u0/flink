/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.table.util

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.typeinfo.{AtomicType, TypeInformation}
import org.apache.flink.api.java.typeutils.{RowTypeInfo, TupleTypeInfo}
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.{LocalStreamEnvironment, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment => ScalaStreamExecEnv}
import org.apache.flink.streaming.api.{TimeCharacteristic, environment}
import org.apache.flink.table.api._
import org.apache.flink.table.api.internal.{TableEnvironmentImpl, TableImpl}
import org.apache.flink.table.api.java.internal.{StreamTableEnvironmentImpl => JavaStreamTableEnvImpl}
import org.apache.flink.table.api.java.{StreamTableEnvironment => JavaStreamTableEnv}
import org.apache.flink.table.api.scala.internal.{StreamTableEnvironmentImpl => ScalaStreamTableEnvImpl}
import org.apache.flink.table.api.scala.{StreamTableEnvironment => ScalaStreamTableEnv}
import org.apache.flink.table.calcite.CalciteConfig
import org.apache.flink.table.catalog.{CatalogManager, FunctionCatalog, GenericInMemoryCatalog}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.delegation.{Executor, ExecutorFactory, PlannerFactory}
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.factories.ComponentFactoryService
import org.apache.flink.table.functions.{AggregateFunction, ScalarFunction, TableFunction, UserFunctionsTypeHelper}
import org.apache.flink.table.operations.{CatalogSinkModifyOperation, DataStreamQueryOperation, ModifyOperation, PlannerQueryOperation, QueryOperation, RichTableSourceQueryOperation}
import org.apache.flink.table.plan.nodes.calcite.LogicalWatermarkAssigner
import org.apache.flink.table.plan.nodes.exec.ExecNode
import org.apache.flink.table.plan.optimize.program.{FlinkBatchProgram, FlinkStreamProgram}
import org.apache.flink.table.plan.stats.FlinkStatistic
import org.apache.flink.table.plan.util.{ExecNodePlanDumper, FlinkRelOptUtil}
import org.apache.flink.table.planner.PlannerBase
import org.apache.flink.table.runtime.utils.{TestingAppendTableSink, TestingRetractTableSink, TestingUpsertTableSink}
import org.apache.flink.table.sinks._
import org.apache.flink.table.sources.{StreamTableSource, TableSource}
import org.apache.flink.table.types.TypeInfoLogicalTypeConverter
import org.apache.flink.table.types.TypeInfoLogicalTypeConverter.fromLogicalTypeToTypeInfo
import org.apache.flink.table.types.logical.LogicalType
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.flink.types.Row

import org.apache.calcite.rel.RelNode
import org.apache.calcite.sql.SqlExplainLevel
import org.apache.commons.lang3.SystemUtils
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Rule
import org.junit.rules.{ExpectedException, TestName}

import _root_.java.util

import _root_.scala.collection.JavaConversions._

/**
  * Test base for testing Table API / SQL plans.
  */
abstract class TableTestBase {

  // used for accurate exception information checking.
  val expectedException: ExpectedException = ExpectedException.none()

  // used for get test case method name
  val testName: TestName = new TestName

  @Rule
  def thrown: ExpectedException = expectedException

  @Rule
  def name: TestName = testName

  def streamTestUtil(): StreamTableTestUtil = StreamTableTestUtil(this)

  def scalaStreamTestUtil(): ScalaStreamTableTestUtil = ScalaStreamTableTestUtil(this)

  def javaStreamTestUtil(): JavaStreamTableTestUtil = JavaStreamTableTestUtil(this)

  def batchTestUtil(): BatchTableTestUtil = BatchTableTestUtil(this)

  def scalaBatchTestUtil(): ScalaBatchTableTestUtil = ScalaBatchTableTestUtil(this)

  def javaBatchTestUtil(): JavaBatchTableTestUtil = JavaBatchTableTestUtil(this)

  def verifyTableEquals(expected: Table, actual: Table): Unit = {
    val expectedString = FlinkRelOptUtil.toString(TableTestUtil.toRelNode(expected))
    val actualString = FlinkRelOptUtil.toString(TableTestUtil.toRelNode(actual))
    assertEquals(
      "Logical plans do not match",
      LogicalPlanFormatUtils.formatTempTableId(expectedString),
      LogicalPlanFormatUtils.formatTempTableId(actualString))
  }
}

abstract class TableTestUtilBase(test: TableTestBase, isBatch: Boolean) {
  protected lazy val diffRepository: DiffRepository = DiffRepository.lookup(test.getClass)

  protected val setting: EnvironmentSettings = if (isBatch) {
    EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build()
  } else {
    EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
  }

  // a counter for unique table names
  private var counter = 0L

  private def getNextId: Long = {
    counter += 1
    counter
  }

  protected def getTableEnv: TableEnvironment

  def getPlanner: PlannerBase = {
    getTableEnv.asInstanceOf[TableEnvironmentImpl].getPlanner.asInstanceOf[PlannerBase]
  }

  def writeToSink(table: Table, sink: TableSink[_], sinkName: String): Unit = {
    val tableEnv = getTableEnv
    tableEnv.registerTableSink(sinkName, sink)
    tableEnv.insertInto(table, sinkName)
  }

  /**
    * Create a [[DataStream]] with the given schema,
    * and registers this DataStream under given name into the TableEnvironment's catalog.
    *
    * @param name table name
    * @param fields field names
    * @tparam T field types
    * @return returns the registered [[Table]].
    */
  def addDataStream[T: TypeInformation](name: String, fields: Expression*): Table = {
    val env = new ScalaStreamExecEnv(new LocalStreamEnvironment())
    val dataStream = env.fromElements[T]().javaStream
    val tableEnv = getTableEnv
    TableTestUtil.registerDataStream(tableEnv, name, dataStream, Some(fields.toArray))
    tableEnv.scan(name)
  }

  /**
    * Create a [[TestTableSource]] with the given schema,
    * and registers this TableSource under a unique name into the TableEnvironment's catalog.
    *
    * @param fields field names
    * @tparam T field types
    * @return returns the registered [[Table]].
    */
  def addTableSource[T: TypeInformation](fields: Expression*): Table = {
    addTableSource[T](s"Table$getNextId", fields: _*)
  }

  /**
    * Create a [[TestTableSource]] with the given schema,
    * and registers this TableSource under given name into the TableEnvironment's catalog.
    *
    * @param name table name
    * @param fields field names
    * @tparam T field types
    * @return returns the registered [[Table]].
    */
  def addTableSource[T: TypeInformation](name: String, fields: Expression*): Table = {
    val typeInfo: TypeInformation[T] = implicitly[TypeInformation[T]]
    val fieldTypes: Array[TypeInformation[_]] = typeInfo match {
      case tt: TupleTypeInfo[_] => (0 until tt.getArity).map(tt.getTypeAt).toArray
      case ct: CaseClassTypeInfo[_] => (0 until ct.getArity).map(ct.getTypeAt).toArray
      case at: AtomicType[_] => Array[TypeInformation[_]](at)
      case _ => throw new TableException(s"Unsupported type info: $typeInfo")
    }
    val fieldsInfo = org.apache.flink.table.typeutils.FieldInfoUtils.getFieldsInfo(
      typeInfo, fields.toArray)
    addTableSource(name, new TestTableSource(isBatch, fieldsInfo.toTableSchema))
  }

  /**
    * Create a [[TestTableSource]] with the given schema, table stats and unique keys,
    * and registers this TableSource under given name into the TableEnvironment's catalog.
    *
    * @param name table name
    * @param types field types
    * @param fields field names
    * @return returns the registered [[Table]].
    */
  def addTableSource(
      name: String,
      types: Array[TypeInformation[_]],
      fields: Array[String]): Table = {
    val schema = new TableSchema(fields, types)
    val tableSource = new TestTableSource(isBatch, schema)
    addTableSource(name, tableSource)
  }

  /**
    * Register this TableSource under given name into the TableEnvironment's catalog.
    *
    * @param name table name
    * @param tableSource table source
    * @return returns the registered [[Table]].
    */
  def addTableSource(
      name: String,
      tableSource: TableSource[_]): Table = {
    getTableEnv.registerTableSource(name, tableSource)
    getTableEnv.scan(name)
  }

  /**
    * Registers a [[ScalarFunction]] under given name into the TableEnvironment's catalog.
    */
  def addFunction(name: String, function: ScalarFunction): Unit = {
    getTableEnv.registerFunction(name, function)
  }

  def verifyPlan(sql: String): Unit = {
    doVerifyPlan(
      sql,
      SqlExplainLevel.EXPPLAN_ATTRIBUTES,
      withRowType = false,
      printPlanBefore = true)
  }

  def verifyPlan(table: Table): Unit = {
    doVerifyPlan(
      table,
      SqlExplainLevel.EXPPLAN_ATTRIBUTES,
      withRowType = false,
      printPlanBefore = true)
  }

  def verifyPlanWithType(sql: String): Unit = {
    doVerifyPlan(
      sql,
      explainLevel = SqlExplainLevel.EXPPLAN_ATTRIBUTES,
      withRowType = true,
      printPlanBefore = true)
  }

  def verifyPlanWithType(table: Table): Unit = {
    doVerifyPlan(
      table,
      explainLevel = SqlExplainLevel.EXPPLAN_ATTRIBUTES,
      withRowType = true,
      printPlanBefore = true)
  }

  def verifyPlanNotExpected(sql: String, notExpected: String*): Unit = {
    verifyPlanNotExpected(getTableEnv.sqlQuery(sql), notExpected: _*)
  }

  def verifyPlanNotExpected(table: Table, notExpected: String*): Unit = {
    require(notExpected.nonEmpty)
    val relNode = TableTestUtil.toRelNode(table)
    val optimizedPlan = getOptimizedPlan(
      Array(relNode),
      explainLevel = SqlExplainLevel.EXPPLAN_ATTRIBUTES,
      withRetractTraits = false,
      withRowType = false)
    val result = notExpected.forall(!optimizedPlan.contains(_))
    val message = s"\nactual plan:\n$optimizedPlan\nnot expected:\n${notExpected.mkString(", ")}"
    assertTrue(message, result)
  }

  def verifyExplain(): Unit = verifyExplain(extended = false)

  def verifyExplain(extended: Boolean): Unit = doVerifyExplain(extended = extended)

  def verifyExplain(sql: String): Unit = verifyExplain(sql, extended = false)

  def verifyExplain(sql: String, extended: Boolean): Unit = {
    val table = getTableEnv.sqlQuery(sql)
    verifyExplain(table, extended)
  }

  def verifyExplain(table: Table): Unit = verifyExplain(table, extended = false)

  def verifyExplain(table: Table, extended: Boolean): Unit = {
    doVerifyExplain(Some(table), extended = extended)
  }

  def doVerifyPlan(
      sql: String,
      explainLevel: SqlExplainLevel,
      withRowType: Boolean,
      printPlanBefore: Boolean): Unit = {
    doVerifyPlan(
      sql = sql,
      explainLevel = explainLevel,
      withRetractTraits = false,
      withRowType = withRowType,
      printPlanBefore = printPlanBefore)
  }

  def doVerifyPlan(
      sql: String,
      explainLevel: SqlExplainLevel,
      withRetractTraits: Boolean,
      withRowType: Boolean,
      printPlanBefore: Boolean): Unit = {
    val table = getTableEnv.sqlQuery(sql)
    val relNode = TableTestUtil.toRelNode(table)
    val optimizedPlan = getOptimizedPlan(
      Array(relNode),
      explainLevel,
      withRetractTraits = withRetractTraits,
      withRowType = withRowType)

    assertEqualsOrExpand("sql", sql)

    if (printPlanBefore) {
      val planBefore = SystemUtils.LINE_SEPARATOR +
        FlinkRelOptUtil.toString(
          relNode,
          SqlExplainLevel.EXPPLAN_ATTRIBUTES,
          withRowType = withRowType)
      assertEqualsOrExpand("planBefore", planBefore)
    }

    val actual = SystemUtils.LINE_SEPARATOR + optimizedPlan
    assertEqualsOrExpand("planAfter", actual.toString, expand = false)
  }

  def verifyResource(sql: String): Unit = {
    assertEqualsOrExpand("sql", sql)
    val table = getTableEnv.sqlQuery(sql)
    doVerifyPlan(
      table,
      explainLevel = SqlExplainLevel.EXPPLAN_ATTRIBUTES,
      withRowType = false,
      withRetractTraits = false,
      printResource = true,
      printPlanBefore = false)
  }

  def doVerifyPlan(
      table: Table,
      explainLevel: SqlExplainLevel,
      withRowType: Boolean,
      printPlanBefore: Boolean): Unit = {
    doVerifyPlan(
      table = table,
      explainLevel = explainLevel,
      withRetractTraits = false,
      withRowType = withRowType,
      printPlanBefore = printPlanBefore)
  }

  def doVerifyPlan(
      table: Table,
      explainLevel: SqlExplainLevel,
      withRowType: Boolean,
      withRetractTraits: Boolean,
      printPlanBefore: Boolean,
      printResource: Boolean = false): Unit = {
    val relNode = TableTestUtil.toRelNode(table)
    val optimizedPlan = getOptimizedPlan(
      Array(relNode),
      explainLevel,
      withRetractTraits = withRetractTraits,
      withRowType = withRowType,
      withResource = printResource)

    if (printPlanBefore) {
      val planBefore = SystemUtils.LINE_SEPARATOR +
        FlinkRelOptUtil.toString(
          relNode,
          SqlExplainLevel.EXPPLAN_ATTRIBUTES,
          withRowType = withRowType)
      assertEqualsOrExpand("planBefore", planBefore)
    }

    val actual = SystemUtils.LINE_SEPARATOR + optimizedPlan
    assertEqualsOrExpand("planAfter", actual.toString, expand = false)
  }

  private def doVerifyExplain(table: Option[Table] = None, extended: Boolean = false): Unit = {
    val explainResult = table match {
      case Some(t) => getTableEnv.explain(t, extended)
      case _ => getTableEnv.explain(extended)
    }
    val actual = if (extended) {
      replaceEstimatedCost(explainResult)
    } else {
      explainResult
    }
    assertEqualsOrExpand("explain", replaceStageId(actual), expand = false)
  }

  protected def getOptimizedPlan(
      relNodes: Array[RelNode],
      explainLevel: SqlExplainLevel,
      withRetractTraits: Boolean,
      withRowType: Boolean,
      withResource: Boolean = false): String = {
    require(relNodes.nonEmpty)
    val planner = getPlanner
    val optimizedRels = planner.optimize(relNodes)
    optimizedRels.head match {
      case _: ExecNode[_, _] =>
        val optimizedNodes = planner.translateToExecNodePlan(optimizedRels)
        require(optimizedNodes.length == optimizedRels.length)
        ExecNodePlanDumper.dagToString(
          optimizedNodes,
          detailLevel = explainLevel,
          withRetractTraits = withRetractTraits,
          withOutputType = withRowType,
          withResource = withResource)
      case _ =>
        optimizedRels.map { rel =>
          FlinkRelOptUtil.toString(
            rel,
            detailLevel = explainLevel,
            withRetractTraits = withRetractTraits,
            withRowType = withRowType)
        }.mkString("\n")
    }
  }

  /**
    * Stage {id} is ignored, because id keeps incrementing in test class
    * while StreamExecutionEnvironment is up
    */
  protected def replaceStageId(s: String): String = {
    s.replaceAll("\\r\\n", "\n").replaceAll("Stage \\d+", "")
  }

  /**
    * ignore estimated cost, because it may be unstable.
    */
  protected def replaceEstimatedCost(s: String): String = {
    var str = s.replaceAll("\\r\\n", "\n")
    val scientificFormRegExpr = "[+-]?[\\d]+([\\.][\\d]*)?([Ee][+-]?[0-9]{0,2})?"
    str = str.replaceAll(s"rowcount = $scientificFormRegExpr", "rowcount = ")
    str = str.replaceAll(s"$scientificFormRegExpr rows", "rows")
    str = str.replaceAll(s"$scientificFormRegExpr cpu", "cpu")
    str = str.replaceAll(s"$scientificFormRegExpr io", "io")
    str = str.replaceAll(s"$scientificFormRegExpr network", "network")
    str = str.replaceAll(s"$scientificFormRegExpr memory", "memory")
    str
  }

  protected def assertEqualsOrExpand(tag: String, actual: String, expand: Boolean = true): Unit = {
    val expected = s"$${$tag}"
    if (!expand) {
      diffRepository.assertEquals(test.name.getMethodName, tag, expected, actual)
      return
    }
    val expanded = diffRepository.expand(test.name.getMethodName, tag, expected)
    if (expanded != null && !expanded.equals(expected)) {
      // expected does exist, check result
      diffRepository.assertEquals(test.name.getMethodName, tag, expected, actual)
    } else {
      // expected does not exist, update
      diffRepository.expand(test.name.getMethodName, tag, actual)
    }
  }
}

abstract class TableTestUtil(
    test: TableTestBase,
    isBatch: Boolean,
    catalogManager: Option[CatalogManager] = None)
  extends TableTestUtilBase(test, isBatch) {
  protected val testingTableEnv: TestingTableEnvironment =
    TestingTableEnvironment.create(setting, catalogManager)
  val tableEnv: TableEnvironment = testingTableEnv

  private val env: StreamExecutionEnvironment = getPlanner.getExecEnv
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  override def getTableEnv: TableEnvironment = tableEnv

  /**
    * Create a [[TestTableSource]] with the given schema, table stats and unique keys,
    * and registers this TableSource under given name into the TableEnvironment's catalog.
    *
    * @param name table name
    * @param types field types
    * @param fields field names
    * @param statistic statistic of current table
    * @return returns the registered [[Table]].
    */
  def addTableSource(
      name: String,
      types: Array[TypeInformation[_]],
      fields: Array[String],
      statistic: FlinkStatistic = FlinkStatistic.UNKNOWN): Table = {
    val schema = new TableSchema(fields, types)
    val tableSource = new TestTableSource(isBatch, schema)
    addTableSource(name, tableSource, statistic)
  }

  /**
    * Register this TableSource under given name into the TableEnvironment's catalog.
    *
    * @param name table name
    * @param tableSource table source
    * @param statistic statistic of current table
    * @return returns the registered [[Table]].
    */
  def addTableSource(
      name: String,
      tableSource: TableSource[_],
      statistic: FlinkStatistic): Table = {
    // TODO RichTableSourceQueryOperation should be deleted and use registerTableSource method
    //  instead of registerTable method here after unique key in TableSchema is ready
    //  and setting catalog statistic to TableSourceTable in DatabaseCalciteSchema is ready
    val operation = new RichTableSourceQueryOperation(tableSource, isBatch, statistic)
    val table = testingTableEnv.createTable(operation)
    testingTableEnv.registerTable(name, table)
    testingTableEnv.scan(name)
  }

  /**
    * Registers a [[TableFunction]] under given name into the TableEnvironment's catalog.
    */
  def addFunction[T: TypeInformation](
      name: String,
      function: TableFunction[T]): Unit = testingTableEnv.registerFunction(name, function)

  /**
    * Registers a [[AggregateFunction]] under given name into the TableEnvironment's catalog.
    */
  def addFunction[T: TypeInformation, ACC: TypeInformation](
      name: String,
      function: AggregateFunction[T, ACC]): Unit = testingTableEnv.registerFunction(name, function)

  def verifyPlan(): Unit = {
    doVerifyPlan(
      SqlExplainLevel.EXPPLAN_ATTRIBUTES,
      withRowType = false,
      withRetractTraits = false,
      printPlanBefore = true)
  }

  def doVerifyPlan(
      explainLevel: SqlExplainLevel,
      withRowType: Boolean,
      withRetractTraits: Boolean,
      printPlanBefore: Boolean): Unit = {
    val testTableEnv = tableEnv.asInstanceOf[TestingTableEnvironment]
    val relNodes = testTableEnv.getBufferedOperations.map(getPlanner.translateToRel)
    if (relNodes.isEmpty) {
      throw new TableException("No output table have been created yet. " +
        "A program needs at least one output table that consumes data.\n" +
        "Please create output table(s) for your program")
    }

    val optimizedPlan = getOptimizedPlan(
      relNodes.toArray,
      explainLevel,
      withRetractTraits = withRetractTraits,
      withRowType = withRowType)
    testTableEnv.clearBufferedOperations()

    if (printPlanBefore) {
      val planBefore = new StringBuilder
      relNodes.foreach { sink =>
        planBefore.append(System.lineSeparator)
        planBefore.append(FlinkRelOptUtil.toString(sink, SqlExplainLevel.EXPPLAN_ATTRIBUTES))
      }
      assertEqualsOrExpand("planBefore", planBefore.toString())
    }

    val actual = SystemUtils.LINE_SEPARATOR + optimizedPlan
    assertEqualsOrExpand("planAfter", actual.toString, expand = false)
  }
}

abstract class ScalaTableTestUtil(
    test: TableTestBase,
    isBatch: Boolean)
  extends TableTestUtilBase(test, isBatch) {
  // scala env
  val env = new ScalaStreamExecEnv(new LocalStreamEnvironment())
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  // scala tableEnv
  val tableEnv: ScalaStreamTableEnv = ScalaStreamTableEnv.create(env, setting)

  override def getTableEnv: TableEnvironment = tableEnv

  /**
    * Registers a [[TableFunction]] under given name into the TableEnvironment's catalog.
    */
  def addFunction[T: TypeInformation](
      name: String,
      function: TableFunction[T]): Unit = tableEnv.registerFunction(name, function)

  /**
    * Registers a [[AggregateFunction]] under given name into the TableEnvironment's catalog.
    */
  def addFunction[T: TypeInformation, ACC: TypeInformation](
      name: String,
      function: AggregateFunction[T, ACC]): Unit = tableEnv.registerFunction(name, function)
}

abstract class JavaTableTestUtil(
    test: TableTestBase,
    isBatch: Boolean)
  extends TableTestUtilBase(test, isBatch) {
  // java env
  val env = new LocalStreamEnvironment()
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  // java tableEnv
  // use impl class instead of interface class to avoid
  // "Static methods in interface require -target:jvm-1.8"
  val tableEnv: JavaStreamTableEnv = JavaStreamTableEnvImpl.create(env, setting, new TableConfig)

  override def getTableEnv: TableEnvironment = tableEnv

  /**
    * Registers a [[TableFunction]] under given name into the TableEnvironment's catalog.
    */
  def addFunction[T: TypeInformation](
      name: String,
      function: TableFunction[T]): Unit = tableEnv.registerFunction(name, function)

  /**
    * Registers a [[AggregateFunction]] under given name into the TableEnvironment's catalog.
    */
  def addFunction[T: TypeInformation, ACC: TypeInformation](
      name: String,
      function: AggregateFunction[T, ACC]): Unit = tableEnv.registerFunction(name, function)
}

/**
  * Utility for stream table test.
  */
case class StreamTableTestUtil(
    test: TableTestBase,
    catalogManager: Option[CatalogManager] = None)
  extends TableTestUtil(test, false, catalogManager) {

  /**
    * Register a table with specific row time field and offset.
    *
    * @param tableName table name
    * @param sourceTable table to register
    * @param rowtimeField row time field
    * @param offset offset to the row time field value
    */
  def addTableWithWatermark(
      tableName: String,
      sourceTable: Table,
      rowtimeField: String,
      offset: Long): Unit = {
    val sourceRel = TableTestUtil.toRelNode(sourceTable)
    val rowtimeFieldIdx = sourceRel.getRowType.getFieldNames.indexOf(rowtimeField)
    if (rowtimeFieldIdx < 0) {
      throw new TableException(s"$rowtimeField does not exist, please check it")
    }
    val watermarkAssigner = new LogicalWatermarkAssigner(
      sourceRel.getCluster,
      sourceRel.getTraitSet,
      sourceRel,
      Some(rowtimeFieldIdx),
      Option(offset)
    )
    val queryOperation = new PlannerQueryOperation(watermarkAssigner)
    testingTableEnv.registerTable(tableName, testingTableEnv.createTable(queryOperation))
  }

  def verifyPlanWithTrait(): Unit = {
    doVerifyPlan(
      SqlExplainLevel.EXPPLAN_ATTRIBUTES,
      withRetractTraits = true,
      withRowType = false,
      printPlanBefore = true)
  }

  def verifyPlanWithTrait(sql: String): Unit = {
    doVerifyPlan(
      sql,
      SqlExplainLevel.EXPPLAN_ATTRIBUTES,
      withRetractTraits = true,
      withRowType = false,
      printPlanBefore = true)
  }

  def verifyPlanWithTrait(table: Table): Unit = {
    doVerifyPlan(
      table,
      SqlExplainLevel.EXPPLAN_ATTRIBUTES,
      withRetractTraits = true,
      withRowType = false,
      printPlanBefore = true)
  }

  def buildStreamProgram(firstProgramNameToRemove: String): Unit = {
    val program = FlinkStreamProgram.buildProgram(tableEnv.getConfig.getConf)
    var startRemove = false
    program.getProgramNames.foreach {
      name =>
        if (name.equals(firstProgramNameToRemove)) {
          startRemove = true
        }
        if (startRemove) {
          program.remove(name)
        }
    }
    val calciteConfig = CalciteConfig.createBuilder(tableEnv.getConfig.getCalciteConfig)
      .replaceStreamProgram(program).build()
    tableEnv.getConfig.setCalciteConfig(calciteConfig)
  }

  def enableMiniBatch(): Unit = {
    tableEnv.getConfig.getConf.setBoolean(
      ExecutionConfigOptions.SQL_EXEC_MINIBATCH_ENABLED, true)
    tableEnv.getConfig.getConf.setString(
      ExecutionConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY, "1 s")
    tableEnv.getConfig.getConf.setLong(ExecutionConfigOptions.SQL_EXEC_MINIBATCH_SIZE, 3L)
  }

  def createAppendTableSink(
      fieldNames: Array[String],
      fieldTypes: Array[LogicalType]): AppendStreamTableSink[Row] = {
    require(fieldNames.length == fieldTypes.length)
    val typeInfos = fieldTypes.map(fromLogicalTypeToTypeInfo)
    new TestingAppendTableSink().configure(fieldNames, typeInfos)
  }

  def createUpsertTableSink(
      keys: Array[Int],
      fieldNames: Array[String],
      fieldTypes: Array[LogicalType]): UpsertStreamTableSink[BaseRow] = {
    require(fieldNames.length == fieldTypes.length)
    val typeInfos = fieldTypes.map(fromLogicalTypeToTypeInfo)
    new TestingUpsertTableSink(keys).configure(fieldNames, typeInfos)
  }

  def createRetractTableSink(
      fieldNames: Array[String],
      fieldTypes: Array[LogicalType]): RetractStreamTableSink[Row] = {
    require(fieldNames.length == fieldTypes.length)
    val typeInfos = fieldTypes.map(fromLogicalTypeToTypeInfo)
    new TestingRetractTableSink().configure(fieldNames, typeInfos)
  }
}

/**
  * Utility for stream scala table test.
  */
case class ScalaStreamTableTestUtil(test: TableTestBase) extends ScalaTableTestUtil(test, false) {
}

/**
  * Utility for stream java table test.
  */
case class JavaStreamTableTestUtil(test: TableTestBase) extends JavaTableTestUtil(test, false) {

}

/**
  * Utility for batch table test.
  */
case class BatchTableTestUtil(
    test: TableTestBase,
    catalogManager: Option[CatalogManager] = None)
  extends TableTestUtil(test, true, catalogManager) {

  def buildBatchProgram(firstProgramNameToRemove: String): Unit = {
    val program = FlinkBatchProgram.buildProgram(tableEnv.getConfig.getConf)
    var startRemove = false
    program.getProgramNames.foreach {
      name =>
        if (name.equals(firstProgramNameToRemove)) {
          startRemove = true
        }
        if (startRemove) {
          program.remove(name)
        }
    }
    val calciteConfig = CalciteConfig.createBuilder(tableEnv.getConfig.getCalciteConfig)
      .replaceBatchProgram(program).build()
    tableEnv.getConfig.setCalciteConfig(calciteConfig)
  }

  def createCollectTableSink(
      fieldNames: Array[String],
      fieldTypes: Array[LogicalType]): TableSink[Row] = {
    require(fieldNames.length == fieldTypes.length)
    val typeInfos = fieldTypes.map(fromLogicalTypeToTypeInfo)
    new CollectRowTableSink().configure(fieldNames, typeInfos)
  }
}

/**
  * Utility for batch scala table test.
  */
case class ScalaBatchTableTestUtil(test: TableTestBase) extends ScalaTableTestUtil(test, true) {
}

/**
  * Utility for batch java table test.
  */
case class JavaBatchTableTestUtil(test: TableTestBase) extends JavaTableTestUtil(test, true) {
}

/**
  * Batch/Stream [[org.apache.flink.table.sources.TableSource]] for testing.
  */
class TestTableSource(isBatch: Boolean, schema: TableSchema)
  extends StreamTableSource[Row] {

  override def isBounded: Boolean = isBatch

  override def getDataStream(
      execEnv: environment.StreamExecutionEnvironment): DataStream[Row] = {
    execEnv.fromCollection(List[Row](), getReturnType)
  }

  override def getReturnType: TypeInformation[Row] = {
    val logicalTypes = schema.getFieldTypes
    new RowTypeInfo(logicalTypes, schema.getFieldNames)
  }

  override def getTableSchema: TableSchema = schema
}

class TestingTableEnvironment private(
    catalogManager: CatalogManager,
    tableConfig: TableConfig,
    executor: Executor,
    functionCatalog: FunctionCatalog,
    planner: PlannerBase,
    isStreaming: Boolean)
  extends TableEnvironmentImpl(
    catalogManager,
    tableConfig,
    executor,
    functionCatalog,
    planner,
    isStreaming) {

  private val bufferedOperations: util.List[ModifyOperation] = new util.ArrayList[ModifyOperation]

  def getBufferedOperations: util.List[ModifyOperation] = bufferedOperations

  def clearBufferedOperations(): Unit = bufferedOperations.clear()

  // just for testing, remove this method while
  // `<T, ACC> void registerFunction(String name, AggregateFunction<T, ACC> aggregateFunction);`
  // is added into TableEnvironment
  def registerFunction[T: TypeInformation](name: String, tf: TableFunction[T]): Unit = {
    val typeInfo = UserFunctionsTypeHelper
      .getReturnTypeOfTableFunction(tf, implicitly[TypeInformation[T]])
    functionCatalog.registerTableFunction(
      name,
      tf,
      typeInfo
    )
  }

  // just for testing, remove this method while
  // `<T> void registerFunction(String name, TableFunction<T> tableFunction);`
  // is added into TableEnvironment
  def registerFunction[T: TypeInformation, ACC: TypeInformation](
      name: String,
      f: AggregateFunction[T, ACC]): Unit = {
    val typeInfo = UserFunctionsTypeHelper
      .getReturnTypeOfAggregateFunction(f, implicitly[TypeInformation[T]])
    val accTypeInfo = UserFunctionsTypeHelper
      .getAccumulatorTypeOfAggregateFunction(f, implicitly[TypeInformation[ACC]])
    functionCatalog.registerAggregateFunction(
      name,
      f,
      typeInfo,
      accTypeInfo
    )
  }

  override def insertInto(table: Table, path: String, pathContinued: String*): Unit = {
    val fullPath = List(path)
    fullPath.addAll(pathContinued)

    val modifyOperations = List(new CatalogSinkModifyOperation(fullPath, table.getQueryOperation))
    if (isEagerOperationTranslation) {
      translate(modifyOperations)
    } else {
      buffer(modifyOperations)
    }
  }

  override def sqlUpdate(stmt: String): Unit = {
    val operations = planner.parse(stmt)
    if (operations.size != 1) {
      throw new TableException(
        "Unsupported SQL query! sqlUpdate() only accepts a single SQL statement of type INSERT.")
    }
    val operation = operations.get(0)
    operation match {
      case modifyOperation: ModifyOperation =>
        val modifyOperations = List(modifyOperation)
        if (isEagerOperationTranslation) {
          translate(modifyOperations)
        } else {
          buffer(modifyOperations)
        }
      case _ => throw new TableException(
        "Unsupported SQL query! sqlUpdate() only accepts a single SQL statements of type INSERT.")
    }
  }

  override def explain(extended: Boolean): String = {
    planner.explain(bufferedOperations.toList, extended)
  }

  @throws[Exception]
  override def execute(jobName: String): JobExecutionResult = {
    translate(bufferedOperations.toList)
    bufferedOperations.clear()
    execEnv.execute(jobName)
  }

  // for test
  def translate(): Unit = {
    translate(bufferedOperations.toList)
  }

  override def createTable(tableOperation: QueryOperation): TableImpl = {
    super.createTable(tableOperation)
  }

  private def translate(modifyOperations: List[ModifyOperation]): Unit = {
    val transformations = planner.translate(modifyOperations)
    execEnv.apply(transformations)
  }

  private def buffer(modifyOperations: List[ModifyOperation]): Unit = {
    bufferedOperations.addAll(modifyOperations)
  }

}

object TestingTableEnvironment {

  def create(
      settings: EnvironmentSettings,
      catalogManager: Option[CatalogManager] = None): TestingTableEnvironment = {
    val functionCatalog = new FunctionCatalog(
      settings.getBuiltInCatalogName, settings.getBuiltInDatabaseName)
    val catalogMgr = catalogManager match {
      case Some(c) => c
      case _ =>
        new CatalogManager(settings.getBuiltInCatalogName,
          new GenericInMemoryCatalog(
            settings.getBuiltInCatalogName, settings.getBuiltInDatabaseName))
    }
    val plannerProperties = settings.toPlannerProperties
    val executorProperties = settings.toExecutorProperties
    val executor = ComponentFactoryService.find(classOf[ExecutorFactory],
      executorProperties).create(executorProperties)
    val tableConfig = new TableConfig
    val planner = ComponentFactoryService.find(classOf[PlannerFactory], plannerProperties)
      .create(plannerProperties, executor, tableConfig, functionCatalog, catalogMgr)
      .asInstanceOf[PlannerBase]
    new TestingTableEnvironment(
      catalogMgr, tableConfig, executor, functionCatalog, planner, !settings.isBatchMode)
  }
}


object TableTestUtil {

  /**
    * Converts operation tree in the given table to a RelNode tree.
    */
  def toRelNode(table: Table): RelNode = {
    table.asInstanceOf[TableImpl]
      .getTableEnvironment.asInstanceOf[TableEnvironmentImpl]
      .getPlanner.asInstanceOf[PlannerBase]
      .getRelBuilder.queryOperation(table.getQueryOperation).build()
  }

  def registerDataStream[T](
      tEnv: TableEnvironment,
      name: String,
      dataStream: DataStream[T],
      fields: Option[Array[Expression]] = None,
      fieldNullables: Option[Array[Boolean]] = None,
      statistic: Option[FlinkStatistic] = None): Unit = {
    val planner = tEnv.asInstanceOf[TableEnvironmentImpl].getPlanner.asInstanceOf[PlannerBase]
    val execEnv = planner.getExecEnv
    val streamType = dataStream.getType
    // get field names and types for all non-replaced fields
    val typeInfoSchema = fields.map((f: Array[Expression]) => {
      val fieldsInfo = org.apache.flink.table.typeutils.FieldInfoUtils.getFieldsInfo(streamType, f)
      // check if event-time is enabled
      if (fieldsInfo.isRowtimeDefined &&
        (execEnv.getStreamTimeCharacteristic ne TimeCharacteristic.EventTime)) {
        throw new ValidationException(String.format(
          "A rowtime attribute requires an EventTime time characteristic in stream " +
            "environment. But is: %s",
          execEnv.getStreamTimeCharacteristic))
      }
      fieldsInfo
    }).getOrElse(org.apache.flink.table.typeutils.FieldInfoUtils.getFieldsInfo(streamType))

    val fieldCnt = typeInfoSchema.getFieldTypes.length
    val dataStreamQueryOperation = new DataStreamQueryOperation(
      dataStream,
      typeInfoSchema.getIndices,
      typeInfoSchema.toTableSchema,
      fieldNullables.getOrElse(Array.fill(fieldCnt)(true)),
      false,
      false,
      statistic.getOrElse(FlinkStatistic.UNKNOWN)
    )
    val table = createTable(tEnv, dataStreamQueryOperation)
    tEnv.registerTable(name, table)
  }

  def createTable(tEnv: TableEnvironment, queryOperation: QueryOperation): Table = {
    val createTableMethod = tEnv match {
      case _: ScalaStreamTableEnvImpl | _: JavaStreamTableEnvImpl =>
        tEnv.getClass.getSuperclass.getDeclaredMethod("createTable", classOf[QueryOperation])
      case t: TableEnvironmentImpl =>
        t.getClass.getDeclaredMethod("createTable", classOf[QueryOperation])
      case _ => throw new TableException(s"Unsupported class: ${tEnv.getClass.getCanonicalName}")
    }
    createTableMethod.setAccessible(true)
    createTableMethod.invoke(tEnv, queryOperation).asInstanceOf[Table]
  }
}
