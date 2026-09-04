"""Spark execution engine (ADR-0005, FF-08).

After schema binding, ``compile`` returns a lazy ``pyspark.sql.DataFrame`` and
performs no collection or write action. Schema discovery happens before this
engine boundary and may run format-specific inference. The adapter never
mutates the user's ``SparkSession``; storage credentials reach Hadoop only
through the explicit :meth:`SparkEngine.apply_storage_credentials` helper.
"""

from __future__ import annotations

import datetime as _dt
from collections.abc import Mapping
from decimal import Decimal

from pyspark.sql import Column as SparkColumn
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from invariantql.adapters.spark_engine.formats import (
    NATIVE_FORMATS,
    csv_options,
    json_options,
    parquet_options,
)
from invariantql.domain.capabilities import EngineCapabilities, PushdownCapabilities
from invariantql.domain.diagnostics import (
    DiagnosticCode,
    ExecutionError,
    UnsupportedOperationError,
)
from invariantql.domain.execution import ExecutionPlan
from invariantql.domain.expressions import (
    Alias,
    And,
    Arithmetic,
    ArithmeticOp,
    Column,
    Comparison,
    ComparisonOp,
    Expression,
    In,
    IsNull,
    Like,
    Literal,
    Not,
    Or,
    Parameter,
    and_all,
    substitute_parameters,
)
from invariantql.domain.redaction import redact_exception
from invariantql.domain.schema import Field, Schema
from invariantql.domain.semantics import expression_type
from invariantql.domain.types import (
    BinaryType,
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    FloatType,
    IntegerType,
    ListType,
    NullType,
    StringType,
    StructType,
    TimestampType,
    UnknownType,
)
from invariantql.ports.engine import Reachability
from invariantql.ports.format_handler import DistributedFormatHandler, ReaderSpec
from invariantql.ports.source import DataSource, FileRelation, NativeRelation
from invariantql.ports.storage import Storage

# NativeRelation kinds the Spark engine knows how to read, and the Spark data
# source format each one maps to. Connector jars must be present on the cluster.
NATIVE_KINDS: dict[str, tuple[str, str]] = {
    "jdbc:postgresql": ("jdbc", "org.postgresql:postgresql JDBC driver"),
    "jdbc:mysql": ("jdbc", "com.mysql:mysql-connector-j JDBC driver"),
    "mongodb": ("mongodb", "org.mongodb.spark:mongo-spark-connector_2.12"),
    "neo4j": (
        "org.neo4j.spark.DataSource",
        "org.neo4j:neo4j-connector-apache-spark_2.12 (_for_spark_3 release)",
    ),
}

_AZURE_AUTHORITIES = {
    "core.windows.net": "login.microsoftonline.com",
    "core.chinacloudapi.cn": "login.chinacloudapi.cn",
    "core.usgovcloudapi.net": "login.microsoftonline.us",
}


class SparkEngine:
    handler_kind = "distributed"

    def __init__(self, spark: SparkSession, *, name: str = "spark") -> None:
        self._spark = spark
        self._name = name
        self._handlers: dict[str, DistributedFormatHandler] = {}

    # -- identity -----------------------------------------------------------

    @property
    def name(self) -> str:
        return self._name

    @property
    def spark(self) -> SparkSession:
        return self._spark

    def register_format_handler(self, handler: DistributedFormatHandler) -> None:
        self._handlers[handler.format_name] = handler

    @property
    def format_handlers(self) -> tuple[str, ...]:
        return tuple(sorted({*NATIVE_FORMATS, *self._handlers}))

    def capabilities(self) -> EngineCapabilities:
        return EngineCapabilities(
            self._name,
            lazy=True,
            evidence=(
                "Spark evaluates residual work lazily; after schema binding, compile() "
                "performs no collection or write action",
            ),
        )

    def reachability(self, source: DataSource) -> Reachability:
        relation = source.relation()
        if isinstance(relation, FileRelation):
            uri = relation.storage.native_uri(relation.location)
            if uri is None:
                return Reachability(
                    False,
                    f"storage {relation.storage.name!r} exposes no engine-visible URI; "
                    "stage the data to a Spark-readable location first",
                )
            fmt = relation.data_format.format_name
            if fmt not in NATIVE_FORMATS and fmt not in self._handlers:
                return Reachability(False, f"no Spark reader registered for format {fmt!r}")
            return Reachability(True, f"Spark reads {uri}")
        if relation.kind in NATIVE_KINDS:
            return Reachability(True, f"Spark connector for {relation.kind}")
        return Reachability(False, f"Spark has no reader for native source kind {relation.kind!r}")

    def scan_capabilities(self, source: DataSource) -> PushdownCapabilities:
        relation = source.relation()
        if isinstance(relation, FileRelation):
            return PushdownCapabilities.full(
                f"Spark {relation.data_format.format_name} reader; Spark's optimizer pushes "
                "projection, predicates and limit (inspect DataFrame.explain() for physical evidence)",
            )
        return PushdownCapabilities.full(
            f"Spark {relation.kind} connector; the connector negotiates pushdown with the source",
        )

    def schema(self, source: DataSource) -> Schema:
        relation = source.relation()
        if isinstance(relation, NativeRelation):
            return source.schema()
        declared = getattr(relation.data_format, "schema", None)
        if isinstance(declared, Schema):
            return declared
        try:
            return from_spark_schema(self._load_file(relation).schema)
        except Exception as exc:
            raise ExecutionError(
                f"Spark could not read the schema of {relation.location.uri}: {redact_exception(exc)}",
                code=DiagnosticCode.SOURCE_SCHEMA_UNAVAILABLE,
                target=source.name,
            ) from None

    # -- compilation --------------------------------------------------------

    def compile(
        self,
        execution_plan: ExecutionPlan,
        source: DataSource,
        parameters: Mapping[str, Literal],
    ) -> DataFrame:
        if not execution_plan.executable:
            raise UnsupportedOperationError(
                "execution plan is not executable on Spark",
                code=DiagnosticCode.ENGINE_PLAN_NOT_EXECUTABLE,
                target=self._name,
            )
        relation = source.relation()
        try:
            df = (
                self._load_file(relation)
                if isinstance(relation, FileRelation)
                else self._load_native(relation)
            )
        except UnsupportedOperationError:
            raise
        except Exception as exc:
            raise ExecutionError(
                f"Spark could not build a reader for {source.name!r}: {redact_exception(exc)}",
                target=self._name,
            ) from None

        values = dict(parameters)
        pushed, residual = execution_plan.pushed, execution_plan.residual
        predicate = and_all(p for p in (pushed.predicate, residual.predicate) if p is not None)
        if predicate is not None:
            df = df.filter(
                to_spark_column(
                    substitute_parameters(predicate, values),
                    execution_plan.schema,
                )
            )
        if residual.projection is not None:
            projected: list[SparkColumn] = []
            for expression, field in zip(
                residual.projection, execution_plan.output_schema, strict=True
            ):
                column = to_spark_column(
                    substitute_parameters(expression, values),
                    execution_plan.schema,
                )
                if not isinstance(field.data_type, (UnknownType, NullType)):
                    column = column.cast(to_spark_type(field.data_type))
                projected.append(column.alias(field.name))
            df = df.select(*projected)
        elif pushed.projection is not None:
            df = df.select(*[F.col(_quote(c)) for c in pushed.projection])
        limits = [n for n in (pushed.limit, residual.limit) if n is not None]
        if limits:
            df = df.limit(min(limits))
        return _normalise_spark_output(df, execution_plan.output_schema)

    def close(self) -> None:
        return None

    # -- explicit session helper (never implicit) ----------------------------

    def apply_storage_credentials(self, storage: Storage) -> dict[str, str]:
        """Copy a storage adapter's credentials into Hadoop configuration.

        This is the only code path that mutates the session. It returns the
        configuration keys it set so callers can audit or undo them.
        """

        options = storage.native_options().reveal()
        jsc = getattr(self._spark.sparkContext, "_jsc", None)
        if jsc is None:
            raise ExecutionError("Spark session has no JVM context; credentials cannot be applied")
        conf = jsc.hadoopConfiguration()
        applied: dict[str, str] = {}
        account = options.get("account_name")
        if account:
            endpoint_kind = options.get("endpoint_kind", "dfs")
            endpoint_suffix = options.get("endpoint_suffix", "core.windows.net")
            host = f"{account}.{endpoint_kind}.{endpoint_suffix}"
            if options.get("account_key"):
                applied[f"fs.azure.account.key.{host}"] = options["account_key"]
                if endpoint_kind == "dfs":
                    applied[f"fs.azure.account.auth.type.{host}"] = "SharedKey"
            elif options.get("sas_token"):
                if endpoint_kind == "blob":
                    container = options.get("container")
                    if container:
                        applied[f"fs.azure.sas.{container}.{host}"] = options["sas_token"]
                else:
                    applied[f"fs.azure.account.auth.type.{host}"] = "SAS"
                    applied[f"fs.azure.sas.token.provider.type.{host}"] = (
                        "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider"
                    )
                    applied[f"fs.azure.sas.fixed.token.{host}"] = options["sas_token"]
            elif (
                endpoint_kind == "dfs"
                and options.get("client_id")
                and options.get("client_secret")
                and options.get("tenant_id")
            ):
                applied[f"fs.azure.account.auth.type.{host}"] = "OAuth"
                applied[f"fs.azure.account.oauth.provider.type.{host}"] = (
                    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
                )
                applied[f"fs.azure.account.oauth2.client.id.{host}"] = options["client_id"]
                applied[f"fs.azure.account.oauth2.client.secret.{host}"] = options["client_secret"]
                applied[f"fs.azure.account.oauth2.client.endpoint.{host}"] = (
                    f"https://{_AZURE_AUTHORITIES.get(endpoint_suffix, 'login.microsoftonline.com')}"
                    f"/{options['tenant_id']}/oauth2/token"
                )
        if options.get("aws_access_key_id"):
            applied["fs.s3a.access.key"] = options["aws_access_key_id"]
        if options.get("aws_secret_access_key"):
            applied["fs.s3a.secret.key"] = options["aws_secret_access_key"]
        if options.get("aws_session_token"):
            applied["fs.s3a.session.token"] = options["aws_session_token"]
            applied["fs.s3a.aws.credentials.provider"] = (
                "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider"
            )
        elif options.get("aws_access_key_id") and options.get("aws_secret_access_key"):
            applied["fs.s3a.aws.credentials.provider"] = (
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
            )
        elif options.get("aws_anonymous") == "true":
            applied["fs.s3a.aws.credentials.provider"] = (
                "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider"
            )
        if options.get("aws_endpoint_url"):
            applied["fs.s3a.endpoint"] = options["aws_endpoint_url"]
        if options.get("aws_allow_http") == "true":
            applied["fs.s3a.connection.ssl.enabled"] = "false"
        if options.get("aws_region"):
            applied["fs.s3a.endpoint.region"] = options["aws_region"]
        for key, value in applied.items():
            conf.set(key, value)
        return applied

    # -- readers ------------------------------------------------------------

    def _load_file(self, relation: FileRelation) -> DataFrame:
        uri = relation.storage.native_uri(relation.location)
        if uri is None:
            raise UnsupportedOperationError(
                f"storage {relation.storage.name!r} exposes no Spark-visible URI",
                code=DiagnosticCode.STAGING_REQUIRED,
                target=self._name,
            )
        spec = self._reader_spec(relation, uri)
        reader = self._spark.read.format(spec.format).options(**spec.options)
        if spec.schema is not None:
            reader = reader.schema(to_spark_schema(spec.schema))
        return reader.load(uri)

    def _reader_spec(self, relation: FileRelation, uri: str) -> ReaderSpec:
        fmt = relation.data_format
        if fmt.format_name == "csv":
            return csv_options(fmt)  # type: ignore[arg-type]
        if fmt.format_name == "parquet":
            return parquet_options(fmt)  # type: ignore[arg-type]
        if fmt.format_name == "json":
            return json_options(fmt)  # type: ignore[arg-type]
        handler = self._handlers.get(fmt.format_name)
        if handler is None:
            raise UnsupportedOperationError(
                f"no Spark reader registered for format {fmt.format_name!r}",
                code=DiagnosticCode.FORMAT_UNSUPPORTED,
                target=self._name,
                details={"format": fmt.format_name},
            )
        return handler.reader_spec(fmt, uri)

    def _load_native(self, relation: NativeRelation) -> DataFrame:
        try:
            spark_format, _requirement = NATIVE_KINDS[relation.kind]
        except KeyError:
            raise UnsupportedOperationError(
                f"Spark has no reader for native source kind {relation.kind!r}",
                code=DiagnosticCode.ENGINE_UNSUPPORTED_SOURCE,
                target=self._name,
            ) from None
        options = {**relation.options, **relation.secrets.reveal()}
        return self._spark.read.format(spark_format).options(**options).load()


# -- expression translation ---------------------------------------------------


def _quote(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def to_spark_column(expression: Expression, schema: Schema | None = None) -> SparkColumn:
    if isinstance(expression, Column):
        return F.col(_quote(expression.name))
    if isinstance(expression, Literal):
        return _literal(expression)
    if isinstance(expression, Parameter):
        raise ExecutionError(
            f"unbound parameter {expression.name!r}", code=DiagnosticCode.PARAMETER_MISSING
        )
    if isinstance(expression, Alias):
        return to_spark_column(expression.expression, schema).alias(expression.name)
    if isinstance(expression, Comparison):
        left = to_spark_column(expression.left, schema)
        right = to_spark_column(expression.right, schema)
        return {
            ComparisonOp.EQ: lambda: left == right,
            ComparisonOp.NE: lambda: left != right,
            ComparisonOp.LT: lambda: left < right,
            ComparisonOp.LE: lambda: left <= right,
            ComparisonOp.GT: lambda: left > right,
            ComparisonOp.GE: lambda: left >= right,
        }[expression.op]()
    if isinstance(expression, And):
        out = to_spark_column(expression.operands[0], schema)
        for operand in expression.operands[1:]:
            out = out & to_spark_column(operand, schema)
        return out
    if isinstance(expression, Or):
        out = to_spark_column(expression.operands[0], schema)
        for operand in expression.operands[1:]:
            out = out | to_spark_column(operand, schema)
        return out
    if isinstance(expression, Not):
        return ~to_spark_column(expression.operand, schema)
    if isinstance(expression, IsNull):
        operand = to_spark_column(expression.operand, schema)
        return operand.isNotNull() if expression.negated else operand.isNull()
    if isinstance(expression, In):
        operand = to_spark_column(expression.operand, schema)
        values = [to_spark_column(v, schema) for v in expression.values]
        result = operand.isin(*values)
        return ~result if expression.negated else result
    if isinstance(expression, Like):
        operand = to_spark_column(expression.operand, schema)
        if not isinstance(expression.pattern, Literal) or not isinstance(
            expression.pattern.value, str
        ):
            raise ExecutionError("LIKE pattern must be a string literal after parameter binding")
        result = operand.like(expression.pattern.value)
        return ~result if expression.negated else result
    if isinstance(expression, Arithmetic):
        left = to_spark_column(expression.left, schema)
        right = to_spark_column(expression.right, schema)
        result_type = expression_type(expression, schema) if schema is not None else None
        if isinstance(result_type, IntegerType):
            left = left.cast(T.LongType())
            right = right.cast(T.LongType())
            if expression.op is ArithmeticOp.ADD:
                return F.try_add(left, right)
            if expression.op is ArithmeticOp.SUB:
                return F.try_subtract(left, right)
            if expression.op is ArithmeticOp.MUL:
                return F.try_multiply(left, right)
        if isinstance(result_type, DecimalType):
            left_type = expression_type(expression.left, schema) if schema is not None else None
            right_type = expression_type(expression.right, schema) if schema is not None else None
            left = left.cast(_spark_decimal_operand_type(left_type, result_type))
            right = right.cast(_spark_decimal_operand_type(right_type, result_type))
        if isinstance(result_type, FloatType) and result_type.bits == 64:
            left = left.cast(T.DoubleType())
            right = right.cast(T.DoubleType())
        if expression.op is ArithmeticOp.ADD:
            result = left + right
        elif expression.op is ArithmeticOp.SUB:
            result = left - right
        elif expression.op is ArithmeticOp.MUL:
            result = left * right
        else:
            denominator = F.when(right == F.lit(0), F.lit(None)).otherwise(right)
            result = left.cast(T.DoubleType()) / denominator
        if isinstance(result_type, DecimalType):
            result = result.cast(to_spark_type(result_type))
        return result
    raise ExecutionError(f"unsupported expression for Spark: {type(expression).__name__}")


def _literal(literal: Literal) -> SparkColumn:
    value = literal.value
    if value is None:
        column = F.lit(None)
        if not isinstance(literal.data_type, (UnknownType, NullType)):
            column = column.cast(to_spark_type(literal.data_type))
        return column
    if isinstance(value, bool | int | float | str | Decimal | _dt.date | _dt.datetime | bytes):
        return F.lit(value).cast(to_spark_type(literal.data_type))
    raise ExecutionError(f"unsupported literal type for Spark: {type(value).__name__}")


def _spark_decimal_operand_type(
    data_type: DataType | None,
    result_type: DecimalType,
) -> T.DecimalType:
    if isinstance(data_type, DecimalType):
        return T.DecimalType(data_type.precision, data_type.scale)
    if isinstance(data_type, IntegerType):
        precision = {8: 3, 16: 5, 32: 10, 64: 19}[data_type.bits]
        return T.DecimalType(precision, 0)
    return T.DecimalType(result_type.precision, result_type.scale)


def _normalise_spark_output(frame: DataFrame, schema: Schema) -> DataFrame:
    """Apply the logical types and conservative nullability without an action."""

    projected: list[SparkColumn] = []
    for field in schema:
        column = F.col(_quote(field.name))
        if not isinstance(field.data_type, (UnknownType, NullType)):
            spark_type = to_spark_type(field.data_type)
            column = column.cast(spark_type)
            null_value = F.lit(None).cast(spark_type)
        else:
            null_value = F.lit(None)
        # CASE preserves values while making nullable result metadata
        # deterministic across file readers and native connectors.
        column = F.when(column.isNull(), null_value).otherwise(column)
        projected.append(column.alias(field.name))
    return frame.select(*projected)


# -- schema translation -------------------------------------------------------


def to_spark_type(data_type: DataType) -> T.DataType:
    if isinstance(data_type, BooleanType):
        return T.BooleanType()
    if isinstance(data_type, IntegerType):
        return {8: T.ByteType(), 16: T.ShortType(), 32: T.IntegerType(), 64: T.LongType()}[
            data_type.bits
        ]
    if isinstance(data_type, FloatType):
        return T.FloatType() if data_type.bits == 32 else T.DoubleType()
    if isinstance(data_type, DecimalType):
        return T.DecimalType(data_type.precision, data_type.scale)
    if isinstance(data_type, StringType):
        return T.StringType()
    if isinstance(data_type, BinaryType):
        return T.BinaryType()
    if isinstance(data_type, DateType):
        return T.DateType()
    if isinstance(data_type, TimestampType):
        return T.TimestampType() if data_type.timezone else T.TimestampNTZType()
    if isinstance(data_type, ListType):
        return T.ArrayType(to_spark_type(data_type.element))
    if isinstance(data_type, StructType):
        return T.StructType([T.StructField(n, to_spark_type(t)) for n, t in data_type.fields])
    if isinstance(data_type, NullType):
        return T.NullType()
    return T.StringType()


def from_spark_type(spark_type: T.DataType) -> DataType:
    if isinstance(spark_type, T.BooleanType):
        return BooleanType()
    if isinstance(spark_type, T.ByteType):
        return IntegerType(8)
    if isinstance(spark_type, T.ShortType):
        return IntegerType(16)
    if isinstance(spark_type, T.IntegerType):
        return IntegerType(32)
    if isinstance(spark_type, T.LongType):
        return IntegerType(64)
    if isinstance(spark_type, T.FloatType):
        return FloatType(32)
    if isinstance(spark_type, T.DoubleType):
        return FloatType(64)
    if isinstance(spark_type, T.DecimalType):
        return DecimalType(spark_type.precision, spark_type.scale)
    if isinstance(spark_type, T.StringType):
        return StringType()
    if isinstance(spark_type, T.BinaryType):
        return BinaryType()
    if isinstance(spark_type, T.DateType):
        return DateType()
    if isinstance(spark_type, T.TimestampNTZType):
        return TimestampType(None)
    if isinstance(spark_type, T.TimestampType):
        return TimestampType("UTC")
    if isinstance(spark_type, T.ArrayType):
        return ListType(from_spark_type(spark_type.elementType))
    if isinstance(spark_type, T.StructType):
        return StructType(tuple((f.name, from_spark_type(f.dataType)) for f in spark_type.fields))
    if isinstance(spark_type, T.NullType):
        return NullType()
    return UnknownType()


def to_spark_schema(schema: Schema) -> T.StructType:
    return T.StructType(
        [T.StructField(f.name, to_spark_type(f.data_type), f.nullable) for f in schema]
    )


def from_spark_schema(schema: T.StructType) -> Schema:
    return Schema(
        tuple(Field(f.name, from_spark_type(f.dataType), f.nullable) for f in schema.fields)
    )


__all__ = [
    "NATIVE_KINDS",
    "SparkEngine",
    "from_spark_schema",
    "from_spark_type",
    "to_spark_column",
    "to_spark_schema",
    "to_spark_type",
]
