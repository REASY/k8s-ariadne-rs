from __future__ import annotations

from enum import StrEnum
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class EntityType(StrEnum):
    POD = "Pod"
    DEPLOYMENT = "Deployment"
    STATEFUL_SET = "StatefulSet"
    REPLICA_SET = "ReplicaSet"
    DAEMON_SET = "DaemonSet"
    JOB = "Job"
    SERVICE = "Service"
    INGRESS = "Ingress"
    ENDPOINT_SLICE = "EndpointSlice"
    NETWORK_POLICY = "NetworkPolicy"
    CONFIG_MAP = "ConfigMap"
    CONTAINER = "Container"
    LOGS = "Logs"
    NODE = "Node"
    NAMESPACE = "Namespace"
    CLUSTER = "Cluster"
    SERVICE_ACCOUNT = "ServiceAccount"
    EVENT = "Event"
    PERSISTENT_VOLUME = "PersistentVolume"
    PERSISTENT_VOLUME_CLAIM = "PersistentVolumeClaim"
    STORAGE_CLASS = "StorageClass"
    PROVISIONER = "Provisioner"
    INGRESS_SERVICE_BACKEND = "IngressServiceBackend"
    ENDPOINT = "Endpoint"
    ENDPOINT_ADDRESS = "EndpointAddress"
    HOST = "Host"
    AWX = "AWX"


class RelationshipType(StrEnum):
    BELONGS_TO = "BelongsTo"
    MANAGES = "Manages"
    RUNS_ON = "RunsOn"
    RUNS = "Runs"
    DEFINES_BACKEND = "DefinesBackend"
    TARGETS_SERVICE = "TargetsService"
    IS_CLAIMED_BY = "IsClaimedBy"
    CONTAINS_ENDPOINT = "ContainsEndpoint"
    HAS_ADDRESS = "HasAddress"
    IS_ADDRESS_OF = "IsAddressOf"
    LISTED_IN = "ListedIn"
    CLAIMS_VOLUME = "ClaimsVolume"
    BOUND_TO = "BoundTo"
    USES_STORAGE_CLASS = "UsesStorageClass"
    USES_PROVISIONER = "UsesProvisioner"
    MOUNTS_CONFIG = "MountsConfig"
    INJECTS_CONFIG = "InjectsConfig"
    USES_IDENTITY = "UsesIdentity"
    APPLIES_TO = "AppliesTo"
    CONCERNS = "Concerns"
    PART_OF = "PartOf"


class FilterOp(StrEnum):
    EQ = "eq"
    NEQ = "neq"
    GT = "gt"
    LT = "lt"
    GTE = "gte"
    LTE = "lte"
    STARTS_WITH = "starts_with"
    ENDS_WITH = "ends_with"
    CONTAINS = "contains"
    IS_NULL = "is_null"
    IS_NOT_NULL = "is_not_null"


class AggregationFn(StrEnum):
    COUNT = "count"
    COUNT_DISTINCT = "count_distinct"
    COLLECT = "collect"
    COLLECT_DISTINCT = "collect_distinct"
    SUM = "sum"
    SUM_MEMORY_MIB = "sum_memory_mib"


class DerivedFn(StrEnum):
    SIZE = "size"


class UnwindElementType(StrEnum):
    K8S_CONTAINER_SPEC = "k8s_container_spec"


class _QPModel(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True, use_enum_values=False)


class RelationshipSpec(_QPModel):
    variable: str | None = None
    relationship: RelationshipType


class PropertyJoin(_QPModel):
    local_property: str
    remote_variable: str
    remote_property: str


class ValueRef(_QPModel):
    variable: str
    property: str


class FilterExpr(_QPModel):
    property: str | None = None
    variable: str | None = None
    alias: str | None = None
    op: FilterOp | None = None
    value: Any | None = None
    value_ref: ValueRef | None = None
    or_: list["FilterExpr"] | None = Field(default=None, alias="or")
    and_: list["FilterExpr"] | None = Field(default=None, alias="and")

    @model_validator(mode="after")
    def _validate_shape(self) -> "FilterExpr":
        forms = 0
        if self.or_ is not None or self.and_ is not None:
            forms += 1
            if self.op is not None or self.alias is not None or self.value_ref is not None:
                raise ValueError("boolean composition cannot be mixed with scalar filter fields")
            if self.or_ is not None and not self.or_:
                raise ValueError("'or' must not be empty")
            if self.and_ is not None and not self.and_:
                raise ValueError("'and' must not be empty")
        elif self.alias is not None:
            forms += 1
            if self.op is None:
                raise ValueError("alias filter requires op")
        elif self.variable is not None and self.property is not None:
            forms += 1
            if self.op is None:
                raise ValueError("variable-scoped property filter requires op")
        elif self.property is not None:
            forms += 1
            if self.op is None:
                raise ValueError("property filter requires op")
        elif self.variable is not None:
            forms += 1
            if self.op not in {FilterOp.IS_NULL, FilterOp.IS_NOT_NULL}:
                raise ValueError("variable-only filters must use is_null or is_not_null")
        if forms != 1:
            raise ValueError("filter must match exactly one supported form")
        return self


class MatchStep(_QPModel):
    entity: EntityType
    bind: str | None = None
    from_: RelationshipSpec | None = Field(default=None, alias="from")
    filter: list[FilterExpr] = Field(default_factory=list)
    not_exists: list["NegationClause"] = Field(default_factory=list)
    property_join: PropertyJoin | None = None
    optional: bool = False


class NegationClause(_QPModel):
    match: list[MatchStep]


class UnwindStep(_QPModel):
    source_variable: str
    source_property: str
    element_type: UnwindElementType
    as_: str = Field(alias="as")


class GroupKey(_QPModel):
    variable: str | None = None
    property: str | None = None
    alias: str | None = None

    @model_validator(mode="after")
    def _validate_shape(self) -> "GroupKey":
        if self.alias is not None and self.variable is None and self.property is None:
            return self
        if self.variable is not None and self.property is None and self.alias is None:
            return self
        if self.variable is not None and self.property is not None and self.alias is not None:
            return self
        raise ValueError("group key must be variable-only, alias-only, or variable+property+alias")


class ComputeExpr(_QPModel):
    fn: AggregationFn | DerivedFn
    input: str
    input_property: str | None = None
    alias: str | None = None


class AggregationStage(_QPModel):
    group_by: list[GroupKey] = Field(default_factory=list)
    compute: list[ComputeExpr] = Field(default_factory=list)
    having: list[FilterExpr] = Field(default_factory=list)


class CoalesceRef(_QPModel):
    variable: str
    property: str


class ReturnExpr(_QPModel):
    variable: str | None = None
    property: str | None = None
    stage_ref: str | None = None
    coalesce: list[CoalesceRef] | None = None
    alias: str | None = None

    @model_validator(mode="after")
    def _validate_shape(self) -> "ReturnExpr":
        forms = 0
        if self.variable is not None or self.property is not None:
            if self.variable is None or self.property is None:
                raise ValueError("property return requires both variable and property")
            forms += 1
        if self.stage_ref is not None:
            forms += 1
        if self.coalesce is not None:
            if len(self.coalesce) < 2:
                raise ValueError("coalesce return requires at least two inputs")
            if self.alias is None:
                raise ValueError("coalesce return requires explicit alias")
            forms += 1
        if forms != 1:
            raise ValueError("return expression must match exactly one supported form")
        return self


class OrderSpec(_QPModel):
    column: str
    direction: Literal["asc", "desc"] = "asc"


class QueryPlanV1(_QPModel):
    schema_name: Literal["QueryPlanV1"] = Field(alias="$schema")
    match: list[MatchStep]
    where: list[FilterExpr] = Field(default_factory=list)
    unwind: UnwindStep | None = None
    stages: list[AggregationStage] = Field(default_factory=list)
    return_: list[ReturnExpr] = Field(alias="return")
    order_by: list[OrderSpec] = Field(default_factory=list)
    limit: int | None = None
    distinct: bool = False


class TranslatorOutput(_QPModel):
    mode: Literal["plan", "cypher"]
    plan: QueryPlanV1 | None = None
    cypher: str | None = None
    reason: str | None = None

    @model_validator(mode="after")
    def _validate_mode(self) -> "TranslatorOutput":
        if self.mode == "plan":
            if self.plan is None or self.cypher is not None or self.reason is not None:
                raise ValueError("plan mode requires plan and forbids cypher/reason")
        else:
            if not self.cypher or not self.reason or self.plan is not None:
                raise ValueError("cypher mode requires cypher+reason and forbids plan")
        return self


MatchStep.model_rebuild()
NegationClause.model_rebuild()
FilterExpr.model_rebuild()

AnyFilterExpr = Annotated[FilterExpr, Field(discriminator=None)]


class QueryPlanV1LiteFilter(_QPModel):
    property: str
    op: Literal["eq"]
    value: str


class QueryPlanV1LiteMatchStep(_QPModel):
    entity: EntityType
    bind: str
    from_: RelationshipSpec | None = Field(default=None, alias="from")
    filter: list[QueryPlanV1LiteFilter] = Field(default_factory=list)


class QueryPlanV1LiteReturnExpr(_QPModel):
    variable: str
    property: str
    alias: str


class QueryPlanV1Lite(_QPModel):
    schema_name: Literal["QueryPlanV1Lite"] = Field(alias="$schema")
    match: list[QueryPlanV1LiteMatchStep]
    return_: list[QueryPlanV1LiteReturnExpr] = Field(alias="return")
    order_by: list[OrderSpec] = Field(default_factory=list)
    distinct: bool = False

    @model_validator(mode="after")
    def _validate_non_empty(self) -> "QueryPlanV1Lite":
        if not self.match:
            raise ValueError("lite plan must contain at least one match step")
        if not self.return_:
            raise ValueError("lite plan must contain at least one return expression")
        return self


def upgrade_lite_plan(plan: QueryPlanV1Lite) -> QueryPlanV1:
    return QueryPlanV1.model_validate(
        {
            "$schema": "QueryPlanV1",
            "match": [
                {
                    "entity": step.entity.value,
                    "bind": step.bind,
                    **({"from": step.from_.model_dump(mode="json", by_alias=True)} if step.from_ else {}),
                    "filter": [
                        {
                            "property": item.property,
                            "op": item.op,
                            "value": item.value,
                        }
                        for item in step.filter
                    ],
                }
                for step in plan.match
            ],
            "return": [
                {
                    "variable": item.variable,
                    "property": item.property,
                    "alias": item.alias,
                }
                for item in plan.return_
            ],
            "order_by": [item.model_dump(mode="json") for item in plan.order_by],
            "distinct": plan.distinct,
        }
    )


class QueryPlanV1MidFilter(_QPModel):
    variable: str
    property: str | None = None
    op: Literal["eq", "is_null", "is_not_null"]
    value: str | None = None

    @model_validator(mode="after")
    def _validate_shape(self) -> "QueryPlanV1MidFilter":
        if self.op == "eq":
            if self.property is None or self.value is None:
                raise ValueError("eq filter requires property and value")
            return self
        if self.property is not None or self.value is not None:
            raise ValueError("null filters must not include property or value")
        return self


class QueryPlanV1MidNegationStep(_QPModel):
    entity: EntityType
    bind: str | None = None
    from_: RelationshipSpec | None = Field(default=None, alias="from")


class QueryPlanV1MidNegationClause(_QPModel):
    match: list[QueryPlanV1MidNegationStep]

    @model_validator(mode="after")
    def _validate_non_empty(self) -> "QueryPlanV1MidNegationClause":
        if not self.match:
            raise ValueError("negation clause must contain at least one match step")
        return self


class QueryPlanV1MidMatchStep(_QPModel):
    entity: EntityType
    bind: str
    from_: RelationshipSpec | None = Field(default=None, alias="from")
    filter: list[QueryPlanV1MidFilter] = Field(default_factory=list)
    optional: bool = False
    property_join: PropertyJoin | None = None
    not_exists: list[QueryPlanV1MidNegationClause] = Field(default_factory=list)


class QueryPlanV1MidReturnExpr(_QPModel):
    variable: str
    property: str
    alias: str


class QueryPlanV1Mid(_QPModel):
    schema_name: Literal["QueryPlanV1Mid"] = Field(alias="$schema")
    match: list[QueryPlanV1MidMatchStep]
    where: list[QueryPlanV1MidFilter] = Field(default_factory=list)
    return_: list[QueryPlanV1MidReturnExpr] = Field(alias="return")
    order_by: list[OrderSpec] = Field(default_factory=list)
    distinct: bool = False

    @model_validator(mode="after")
    def _validate_non_empty(self) -> "QueryPlanV1Mid":
        if not self.match:
            raise ValueError("mid plan must contain at least one match step")
        if not self.return_:
            raise ValueError("mid plan must contain at least one return expression")
        return self


def upgrade_mid_plan(plan: QueryPlanV1Mid) -> QueryPlanV1:
    return QueryPlanV1.model_validate(
        {
            "$schema": "QueryPlanV1",
            "match": [
                {
                    "entity": step.entity.value,
                    "bind": step.bind,
                    **({"from": step.from_.model_dump(mode="json", by_alias=True)} if step.from_ else {}),
                    "filter": [_upgrade_mid_filter(item) for item in step.filter],
                    "optional": step.optional,
                    **(
                        {"property_join": step.property_join.model_dump(mode="json")}
                        if step.property_join
                        else {}
                    ),
                    "not_exists": [
                        {
                            "match": [
                                {
                                    "entity": neg_step.entity.value,
                                    **({"bind": neg_step.bind} if neg_step.bind is not None else {}),
                                    **(
                                        {"from": neg_step.from_.model_dump(mode="json", by_alias=True)}
                                        if neg_step.from_
                                        else {}
                                    ),
                                    "filter": [],
                                }
                                for neg_step in clause.match
                            ]
                        }
                        for clause in step.not_exists
                    ],
                }
                for step in plan.match
            ],
            "where": [_upgrade_mid_filter(item) for item in plan.where],
            "return": [
                {
                    "variable": item.variable,
                    "property": item.property,
                    "alias": item.alias,
                }
                for item in plan.return_
            ],
            "order_by": [item.model_dump(mode="json") for item in plan.order_by],
            "distinct": plan.distinct,
        }
    )


def _upgrade_mid_filter(filter_expr: QueryPlanV1MidFilter) -> dict[str, Any]:
    if filter_expr.op == "eq":
        return {
            "variable": filter_expr.variable,
            "property": filter_expr.property,
            "op": filter_expr.op,
            "value": filter_expr.value,
        }
    return {
        "variable": filter_expr.variable,
        "op": filter_expr.op,
    }
