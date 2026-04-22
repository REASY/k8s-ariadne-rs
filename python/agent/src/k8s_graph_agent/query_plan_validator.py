from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Iterable

from .graph_schema import GraphSchema
from .query_plan import (
    AggregationFn,
    AggregationStage,
    ComputeExpr,
    CoalesceRef,
    DerivedFn,
    EntityType,
    FilterExpr,
    FilterOp,
    GroupKey,
    MatchStep,
    NegationClause,
    QueryPlanV1,
    ReturnExpr,
    TranslatorOutput,
    UnwindElementType,
)


@dataclass(frozen=True)
class QueryPlanIssue:
    code: str
    path: str
    message: str


class QueryPlanValidationError(ValueError):
    def __init__(self, issues: Iterable[QueryPlanIssue]) -> None:
        self.issues = tuple(issues)
        summary = "; ".join(f"{issue.path}: {issue.message}" for issue in self.issues)
        super().__init__(summary or "query plan validation failed")


@dataclass(frozen=True)
class _Symbol:
    kind: str
    type_name: str | None = None


_SHARED_PROPERTIES = {
    "name",
    "namespace",
    "uid",
}

_ENTITY_PROPERTIES: dict[str, set[str]] = {
    entity.value: set(_SHARED_PROPERTIES)
    for entity in EntityType
    if entity != EntityType.HOST
}
_ENTITY_PROPERTIES[EntityType.HOST.value] = {"name"}
_ENTITY_PROPERTIES[EntityType.POD.value] |= {"phase", "spec.containers"}
_ENTITY_PROPERTIES[EntityType.DEPLOYMENT.value] |= {"replicas", "ready_replicas"}
_ENTITY_PROPERTIES[EntityType.STATEFUL_SET.value] |= {"replicas"}
_ENTITY_PROPERTIES[EntityType.CONTAINER.value] |= {
    "container_type",
    "pod_name",
    "container_uid",
}
_ENTITY_PROPERTIES[EntityType.LOGS.value] = {"content", "container_uid"}
_ENTITY_PROPERTIES[EntityType.ENDPOINT_SLICE.value] |= {"address_type"}
_ENTITY_PROPERTIES[EntityType.ENDPOINT_ADDRESS.value] |= {"address"}
_ENTITY_PROPERTIES[EntityType.PERSISTENT_VOLUME.value] |= {
    "phase",
    "storage_class_name",
    "capacity_storage",
}
_ENTITY_PROPERTIES[EntityType.PERSISTENT_VOLUME_CLAIM.value] |= {
    "phase",
    "storage_class_name",
    "volume_name",
}
_ENTITY_PROPERTIES[EntityType.NODE.value] |= {"provider_id"}
_ENTITY_PROPERTIES[EntityType.EVENT.value] |= {"type", "reason", "note", "event_time"}

_UNWIND_PROPERTIES: dict[str, set[str]] = {
    UnwindElementType.K8S_CONTAINER_SPEC.value: {
        "resources.requests.memory",
        "resources.limits.memory",
        "resources.requests.cpu",
        "resources.limits.cpu",
        "name",
        "image",
    }
}


def query_plan_entity_properties() -> dict[str, tuple[str, ...]]:
    return {
        type_name: tuple(sorted(properties))
        for type_name, properties in sorted(_ENTITY_PROPERTIES.items())
    }


def query_plan_unwind_properties() -> dict[str, tuple[str, ...]]:
    return {
        type_name: tuple(sorted(properties))
        for type_name, properties in sorted(_UNWIND_PROPERTIES.items())
    }

_WRITE_CYPHER_PATTERN = re.compile(
    r"\b(CREATE|MERGE|SET|DELETE|DETACH\s+DELETE)\b", flags=re.IGNORECASE
)


def validate_translator_output(
    output: TranslatorOutput, schema: GraphSchema | None = None
) -> None:
    issues: list[QueryPlanIssue] = []
    if output.mode == "cypher":
        if output.cypher and _WRITE_CYPHER_PATTERN.search(output.cypher):
            issues.append(
                QueryPlanIssue(
                    code="write_operation",
                    path="cypher",
                    message="write operations are not allowed in raw cypher fallback",
                )
            )
    elif output.plan is not None:
        issues.extend(validate_query_plan(output.plan, schema=schema, raise_on_error=False))
    if issues:
        raise QueryPlanValidationError(issues)


def validate_query_plan(
    plan: QueryPlanV1,
    schema: GraphSchema | None = None,
    *,
    raise_on_error: bool = True,
) -> tuple[QueryPlanIssue, ...]:
    validator = _QueryPlanValidator(schema or GraphSchema.load_default())
    issues = validator.validate(plan)
    if issues and raise_on_error:
        raise QueryPlanValidationError(issues)
    return issues


class _QueryPlanValidator:
    def __init__(self, schema: GraphSchema) -> None:
        self.schema = schema
        self.issues: list[QueryPlanIssue] = []

    def validate(self, plan: QueryPlanV1) -> tuple[QueryPlanIssue, ...]:
        if not plan.match:
            self._issue("empty_match", "match", "plan must contain at least one match step")
        if not plan.return_:
            self._issue("empty_return", "return", "plan must contain at least one return expression")

        scope, last_step = self._validate_match_steps(plan.match, {}, "match")
        self._validate_filters(plan.where, scope, path="where", current_entity=None, allow_alias=False)

        if plan.unwind is not None:
            self._validate_unwind(plan.unwind, scope, path="unwind")
            scope[plan.unwind.as_] = _Symbol(kind="unwind", type_name=plan.unwind.element_type.value)

        stage_scope = dict(scope)
        for index, stage in enumerate(plan.stages):
            stage_scope = self._validate_stage(stage, stage_scope, f"stages[{index}]")

        final_scope = stage_scope if plan.stages else scope
        return_aliases = self._validate_returns(plan.return_, final_scope, path="return")

        for index, order in enumerate(plan.order_by):
            if order.column not in return_aliases:
                self._issue(
                    "unknown_order_column",
                    f"order_by[{index}].column",
                    f"unknown return alias '{order.column}'",
                )

        return tuple(self.issues)

    def _validate_match_steps(
        self,
        steps: list[MatchStep],
        outer_scope: dict[str, _Symbol],
        path: str,
    ) -> tuple[dict[str, _Symbol], MatchStep | None]:
        scope = dict(outer_scope)
        previous_step: MatchStep | None = None
        for index, step in enumerate(steps):
            step_path = f"{path}[{index}]"
            source_type: str | None = None
            if step.from_ is not None:
                source_type = self._resolve_relationship_source_type(
                    step.from_.variable, previous_step, scope, f"{step_path}.from"
                )
                if source_type is not None and not self._relationship_connects(
                    source_type, step.entity.value, step.from_.relationship.value
                ):
                    self._issue(
                        "illegal_relationship",
                        f"{step_path}.from.relationship",
                        f"{step.from_.relationship.value} does not connect {source_type} and {step.entity.value}",
                    )

            self._validate_filters(
                step.filter,
                scope,
                path=f"{step_path}.filter",
                current_entity=step.entity.value,
                allow_alias=False,
            )
            if step.bind is not None:
                existing = scope.get(step.bind)
                if existing is not None and existing.kind == "entity":
                    if existing.type_name != step.entity.value:
                        self._issue(
                            "bind_type_mismatch",
                            f"{step_path}.bind",
                            f"variable '{step.bind}' already bound as {existing.type_name}, cannot reuse for {step.entity.value}",
                        )
                else:
                    scope[step.bind] = _Symbol(kind="entity", type_name=step.entity.value)
            if step.property_join is not None:
                self._validate_property_name(
                    step.entity.value,
                    step.property_join.local_property,
                    f"{step_path}.property_join.local_property",
                )
                remote = scope.get(step.property_join.remote_variable)
                if remote is None or remote.type_name is None:
                    self._issue(
                        "unknown_variable",
                        f"{step_path}.property_join.remote_variable",
                        f"unknown variable '{step.property_join.remote_variable}'",
                    )
                elif remote.kind not in {"entity", "unwind"}:
                    self._issue(
                        "invalid_property_join_target",
                        f"{step_path}.property_join.remote_variable",
                        "property joins must target an entity-like variable",
                    )
                else:
                    self._validate_property_name(
                        remote.type_name,
                        step.property_join.remote_property,
                        f"{step_path}.property_join.remote_property",
                    )

            for negation_index, negation in enumerate(step.not_exists):
                self._validate_negation(
                    negation,
                    scope,
                    f"{step_path}.not_exists[{negation_index}]",
                )
            previous_step = step
        return scope, previous_step

    def _validate_negation(
        self, negation: NegationClause, outer_scope: dict[str, _Symbol], path: str
    ) -> None:
        if not negation.match:
            self._issue("empty_negation", path, "negation clause must contain at least one match step")
            return
        self._validate_match_steps(negation.match, outer_scope, f"{path}.match")

    def _validate_unwind(
        self, unwind, scope: dict[str, _Symbol], path: str
    ) -> None:
        source = scope.get(unwind.source_variable)
        if source is None or source.type_name is None:
            self._issue(
                "unknown_unwind_source",
                f"{path}.source_variable",
                f"unknown variable '{unwind.source_variable}'",
            )
            return
        self._validate_property_name(
            source.type_name,
            unwind.source_property,
            f"{path}.source_property",
        )

    def _validate_stage(
        self, stage: AggregationStage, scope: dict[str, _Symbol], path: str
    ) -> dict[str, _Symbol]:
        alias_scope: dict[str, _Symbol] = {}
        for index, group_key in enumerate(stage.group_by):
            key_path = f"{path}.group_by[{index}]"
            name = self._validate_group_key(group_key, scope, key_path)
            if name is not None:
                self._record_stage_name(alias_scope, name, key_path)
                if group_key.variable is not None and group_key.property is None:
                    alias_scope[name] = scope[group_key.variable]
                else:
                    alias_scope[name] = _Symbol(kind="alias", type_name=None)

        for index, compute in enumerate(stage.compute):
            compute_path = f"{path}.compute[{index}]"
            alias = self._validate_compute(compute, scope, alias_scope, compute_path)
            self._record_stage_name(alias_scope, alias, compute_path)
            alias_scope[alias] = _Symbol(kind="alias", type_name=None)

        self._validate_filters(
            stage.having,
            alias_scope,
            path=f"{path}.having",
            current_entity=None,
            allow_alias=True,
        )
        return alias_scope

    def _validate_group_key(
        self, group_key: GroupKey, scope: dict[str, _Symbol], path: str
    ) -> str | None:
        if group_key.alias is not None and group_key.variable is None:
            if group_key.alias not in scope:
                self._issue("unknown_alias", f"{path}.alias", f"unknown alias '{group_key.alias}'")
            return group_key.alias
        if group_key.variable is None:
            return None
        symbol = scope.get(group_key.variable)
        if symbol is None or symbol.type_name is None:
            self._issue("unknown_variable", f"{path}.variable", f"unknown variable '{group_key.variable}'")
            return group_key.alias or group_key.variable
        if group_key.property is None:
            return group_key.variable
        self._validate_property_name(
            symbol.type_name,
            group_key.property,
            f"{path}.property",
        )
        return group_key.alias

    def _validate_compute(
        self,
        compute: ComputeExpr,
        base_scope: dict[str, _Symbol],
        stage_scope: dict[str, _Symbol],
        path: str,
    ) -> str:
        input_symbol = base_scope.get(compute.input) or stage_scope.get(compute.input)
        if input_symbol is None:
            self._issue("unknown_input", f"{path}.input", f"unknown input '{compute.input}'")
        if isinstance(compute.fn, DerivedFn):
            if compute.fn != DerivedFn.SIZE:
                self._issue("unsupported_derived_fn", f"{path}.fn", f"unsupported derived fn '{compute.fn.value}'")
            if compute.input not in base_scope and compute.input not in stage_scope:
                self._issue(
                    "derived_input_scope",
                    f"{path}.input",
                    f"derived function input '{compute.input}' must reference a prior stage alias",
                )
        else:
            if compute.input_property is not None:
                if input_symbol is None or input_symbol.type_name is None:
                    self._issue("unknown_input_property", f"{path}.input_property", "cannot resolve property on unknown input")
                else:
                    self._validate_property_name(
                        input_symbol.type_name,
                        compute.input_property,
                        f"{path}.input_property",
                    )
            elif compute.fn == AggregationFn.SUM_MEMORY_MIB:
                self._issue(
                    "missing_input_property",
                    f"{path}.input_property",
                    "sum_memory_mib requires input_property",
                )
        return compute.alias or _default_compute_alias(compute, input_symbol)

    def _validate_returns(
        self, returns: list[ReturnExpr], scope: dict[str, _Symbol], path: str
    ) -> set[str]:
        aliases: set[str] = set()
        collisions: dict[str, int] = {}
        for index, item in enumerate(returns):
            item_path = f"{path}[{index}]"
            alias = self._resolve_return_alias(item, scope, collisions, item_path)
            aliases.add(alias)
        return aliases

    def _resolve_return_alias(
        self,
        item: ReturnExpr,
        scope: dict[str, _Symbol],
        collisions: dict[str, int],
        path: str,
    ) -> str:
        if item.stage_ref is not None:
            if item.stage_ref not in scope:
                self._issue("unknown_stage_ref", f"{path}.stage_ref", f"unknown stage alias '{item.stage_ref}'")
            base = item.alias or item.stage_ref
            return self._dedupe_alias(base, collisions)
        if item.coalesce is not None:
            for idx, ref in enumerate(item.coalesce):
                self._validate_coalesce_ref(ref, scope, f"{path}.coalesce[{idx}]")
            return item.alias or "coalesce"
        assert item.variable is not None and item.property is not None
        symbol = scope.get(item.variable)
        if symbol is None or symbol.type_name is None:
            self._issue("unknown_variable", f"{path}.variable", f"unknown variable '{item.variable}'")
            base = item.alias or item.property
            return self._dedupe_alias(base, collisions)
        self._validate_property_name(symbol.type_name, item.property, f"{path}.property")
        base = item.alias or _default_return_alias(symbol.type_name, item.property)
        return self._dedupe_alias(base, collisions)

    def _validate_coalesce_ref(
        self, ref: CoalesceRef, scope: dict[str, _Symbol], path: str
    ) -> None:
        symbol = scope.get(ref.variable)
        if symbol is None or symbol.type_name is None:
            self._issue("unknown_variable", f"{path}.variable", f"unknown variable '{ref.variable}'")
            return
        self._validate_property_name(symbol.type_name, ref.property, f"{path}.property")

    def _validate_filters(
        self,
        filters: list[FilterExpr],
        scope: dict[str, _Symbol],
        *,
        path: str,
        current_entity: str | None,
        allow_alias: bool,
    ) -> None:
        for index, filter_expr in enumerate(filters):
            self._validate_filter(
                filter_expr,
                scope,
                path=f"{path}[{index}]",
                current_entity=current_entity,
                allow_alias=allow_alias,
            )

    def _validate_filter(
        self,
        filter_expr: FilterExpr,
        scope: dict[str, _Symbol],
        *,
        path: str,
        current_entity: str | None,
        allow_alias: bool,
    ) -> None:
        if filter_expr.or_ is not None:
            for index, nested in enumerate(filter_expr.or_):
                self._validate_filter(
                    nested, scope, path=f"{path}.or[{index}]", current_entity=current_entity, allow_alias=allow_alias
                )
            return
        if filter_expr.and_ is not None:
            for index, nested in enumerate(filter_expr.and_):
                self._validate_filter(
                    nested, scope, path=f"{path}.and[{index}]", current_entity=current_entity, allow_alias=allow_alias
                )
            return
        if filter_expr.alias is not None:
            if not allow_alias:
                self._issue("alias_filter_context", path, "alias filters are only valid in aggregation having clauses")
            elif filter_expr.alias not in scope:
                self._issue("unknown_alias", f"{path}.alias", f"unknown alias '{filter_expr.alias}'")
            return
        if filter_expr.property is not None and filter_expr.variable is None:
            if current_entity is None:
                self._issue("missing_variable", path, "property filter requires a variable in this context")
                return
            self._validate_property_name(current_entity, filter_expr.property, f"{path}.property")
            if filter_expr.value_ref is not None:
                self._validate_value_ref(filter_expr.value_ref, scope, f"{path}.value_ref")
            return
        if filter_expr.variable is not None and filter_expr.property is None:
            symbol = scope.get(filter_expr.variable)
            if symbol is None:
                self._issue("unknown_variable", f"{path}.variable", f"unknown variable '{filter_expr.variable}'")
            return
        if filter_expr.variable is not None and filter_expr.property is not None:
            symbol = scope.get(filter_expr.variable)
            if symbol is None or symbol.type_name is None:
                self._issue("unknown_variable", f"{path}.variable", f"unknown variable '{filter_expr.variable}'")
                return
            self._validate_property_name(symbol.type_name, filter_expr.property, f"{path}.property")
            if filter_expr.value_ref is not None:
                self._validate_value_ref(filter_expr.value_ref, scope, f"{path}.value_ref")

    def _validate_value_ref(self, value_ref, scope: dict[str, _Symbol], path: str) -> None:
        symbol = scope.get(value_ref.variable)
        if symbol is None or symbol.type_name is None:
            self._issue("unknown_variable", f"{path}.variable", f"unknown variable '{value_ref.variable}'")
            return
        self._validate_property_name(symbol.type_name, value_ref.property, f"{path}.property")

    def _resolve_relationship_source_type(
        self,
        variable: str | None,
        previous_step: MatchStep | None,
        scope: dict[str, _Symbol],
        path: str,
    ) -> str | None:
        if variable is not None:
            symbol = scope.get(variable)
            if symbol is None or symbol.type_name is None:
                self._issue("unknown_variable", f"{path}.variable", f"unknown variable '{variable}'")
                return None
            return symbol.type_name
        if previous_step is None:
            self._issue("missing_from_variable", path, "implicit relationship source requires a previous step")
            return None
        return previous_step.entity.value

    def _relationship_connects(self, left: str, right: str, relationship: str) -> bool:
        return self.schema.allows(relationship, left, right) or self.schema.allows(
            relationship, right, left
        )

    def _validate_property_name(self, type_name: str, property_name: str, path: str) -> None:
        if type_name in _ENTITY_PROPERTIES and property_name in _ENTITY_PROPERTIES[type_name]:
            return
        if type_name in _UNWIND_PROPERTIES and property_name in _UNWIND_PROPERTIES[type_name]:
            return
        self._issue(
            "unknown_property",
            path,
            f"property '{property_name}' is not defined for {type_name}",
        )

    def _record_stage_name(
        self, stage_scope: dict[str, _Symbol], name: str, path: str
    ) -> None:
        if name in stage_scope:
            self._issue("duplicate_stage_name", path, f"duplicate stage name '{name}'")

    def _dedupe_alias(self, base: str, collisions: dict[str, int]) -> str:
        count = collisions.get(base, 0) + 1
        collisions[base] = count
        if count == 1:
            return base
        return f"{base}_{count}"

    def _issue(self, code: str, path: str, message: str) -> None:
        self.issues.append(QueryPlanIssue(code=code, path=path, message=message))


def _default_return_alias(type_name: str, property_name: str) -> str:
    snake = _snake_case(type_name)
    if property_name == "name":
        return snake
    return f"{snake}_{property_name.replace('.', '_')}"


def _default_compute_alias(compute: ComputeExpr, input_symbol: _Symbol | None) -> str:
    if isinstance(compute.fn, DerivedFn):
        return f"{compute.input}_size"
    entity_name = _snake_case(input_symbol.type_name or compute.input)
    if compute.fn in {AggregationFn.COUNT, AggregationFn.COUNT_DISTINCT}:
        return f"{entity_name}_count"
    if compute.fn in {AggregationFn.COLLECT, AggregationFn.COLLECT_DISTINCT}:
        return f"{entity_name}_list"
    if compute.fn == AggregationFn.SUM:
        return f"total_{entity_name}"
    if compute.fn == AggregationFn.SUM_MEMORY_MIB:
        suffix = (compute.input_property or "value").replace(".", "_")
        return f"total_{suffix}_mib"
    return f"{entity_name}_{compute.fn.value}"


def _snake_case(value: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", value).lower()
