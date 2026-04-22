from __future__ import annotations

from dataclasses import dataclass
import json
import re

from .graph_schema import GraphSchema
from .query_plan import (
    AggregationFn,
    AggregationStage,
    CoalesceRef,
    ComputeExpr,
    DerivedFn,
    EntityType,
    FilterExpr,
    FilterOp,
    GroupKey,
    MatchStep,
    QueryPlanV1,
    ReturnExpr,
    UnwindElementType,
)
from .query_plan_validator import validate_query_plan


@dataclass(frozen=True)
class CompiledQueryPlan:
    cypher: str


@dataclass
class _Symbol:
    kind: str
    type_name: str


_SHARED_PROPERTIES = {
    "name": "metadata.name",
    "namespace": "metadata.namespace",
    "uid": "metadata.uid",
}

_ENTITY_PROPERTIES: dict[str, dict[str, str]] = {
    entity.value: dict(_SHARED_PROPERTIES)
    for entity in EntityType
    if entity != EntityType.HOST
}
_ENTITY_PROPERTIES[EntityType.HOST.value] = {"name": "name"}
_ENTITY_PROPERTIES[EntityType.POD.value] |= {
    "phase": "status.phase",
    "spec.containers": "spec.containers",
}
_ENTITY_PROPERTIES[EntityType.DEPLOYMENT.value] |= {
    "replicas": "spec.replicas",
    "ready_replicas": "status.readyReplicas",
}
_ENTITY_PROPERTIES[EntityType.STATEFUL_SET.value] |= {"replicas": "spec.replicas"}
_ENTITY_PROPERTIES[EntityType.CONTAINER.value] |= {
    "container_type": "container_type",
    "pod_name": "pod_name",
    "container_uid": "metadata.uid",
}
_ENTITY_PROPERTIES[EntityType.LOGS.value] = {
    "content": "content",
    "container_uid": "container_uid",
}
_ENTITY_PROPERTIES[EntityType.ENDPOINT_SLICE.value] |= {"address_type": "addressType"}
_ENTITY_PROPERTIES[EntityType.ENDPOINT_ADDRESS.value] |= {"address": "address"}
_ENTITY_PROPERTIES[EntityType.PERSISTENT_VOLUME.value] |= {
    "phase": "status.phase",
    "storage_class_name": "spec.storageClassName",
    "capacity_storage": "spec.capacity.storage",
}
_ENTITY_PROPERTIES[EntityType.PERSISTENT_VOLUME_CLAIM.value] |= {
    "phase": "status.phase",
    "storage_class_name": "spec.storageClassName",
    "volume_name": "spec.volumeName",
}
_ENTITY_PROPERTIES[EntityType.NODE.value] |= {
    "phase": "status.phase",
    "provider_id": "spec.providerID",
}
_ENTITY_PROPERTIES[EntityType.EVENT.value] = {
    "type": "type",
    "reason": "reason",
    "note": "note",
    "event_time": "eventTime",
}

_UNWIND_PROPERTIES: dict[str, dict[str, str]] = {
    UnwindElementType.K8S_CONTAINER_SPEC.value: {
        "resources.requests.memory": "resources.requests.memory",
        "resources.limits.memory": "resources.limits.memory",
        "resources.requests.cpu": "resources.requests.cpu",
        "resources.limits.cpu": "resources.limits.cpu",
        "name": "name",
        "image": "image",
    }
}


def compile_query_plan(
    plan: QueryPlanV1, schema: GraphSchema | None = None
) -> CompiledQueryPlan:
    active_schema = schema or GraphSchema.load_default()
    validate_query_plan(plan, schema=active_schema)
    compiler = _QueryPlanCompiler(active_schema)
    return CompiledQueryPlan(cypher=compiler.compile(plan))


class _QueryPlanCompiler:
    def __init__(self, schema: GraphSchema) -> None:
        self.schema = schema
        self._generated = 0

    def compile(self, plan: QueryPlanV1) -> str:
        lines: list[str] = []
        scope: dict[str, _Symbol] = {}
        previous: tuple[str, str] | None = None

        for index, step in enumerate(plan.match):
            bind = step.bind or self._fresh_var(f"m{index}")
            scope[bind] = _Symbol(kind="entity", type_name=step.entity.value)
            pattern, clause_filters = self._compile_match_step(
                step=step,
                bind=bind,
                scope=scope,
                previous=previous,
            )
            keyword = "OPTIONAL MATCH" if step.optional else "MATCH"
            lines.append(f"{keyword} {pattern}")
            if clause_filters:
                lines.append(f"WHERE {' AND '.join(clause_filters)}")
            previous = (bind, step.entity.value)

        if plan.where:
            global_filters = [
                self._compile_filter(filter_expr, scope, current_bind=None, current_type=None)
                for filter_expr in plan.where
            ]
            lines.append(f"WHERE {' AND '.join(global_filters)}")

        if plan.unwind is not None:
            source_expr = self._property_expr(
                plan.unwind.source_variable,
                scope[plan.unwind.source_variable].type_name,
                plan.unwind.source_property,
            )
            lines.append(f"UNWIND {source_expr} AS {plan.unwind.as_}")
            scope[plan.unwind.as_] = _Symbol(
                kind="unwind", type_name=plan.unwind.element_type.value
            )

        stage_scope = dict(scope)
        for index, stage in enumerate(plan.stages):
            stage_lines, stage_scope = self._compile_stage(stage, stage_scope, index)
            lines.extend(stage_lines)

        final_scope = stage_scope if plan.stages else scope
        return_clause = self._compile_return(plan.return_, final_scope, plan.distinct)
        lines.append(return_clause)

        if plan.order_by:
            order_parts = [
                f"{order.column} {order.direction.upper()}" for order in plan.order_by
            ]
            lines.append(f"ORDER BY {', '.join(order_parts)}")
        if plan.limit is not None:
            lines.append(f"LIMIT {plan.limit}")
        return "\n".join(lines)

    def _compile_match_step(
        self,
        *,
        step: MatchStep,
        bind: str,
        scope: dict[str, _Symbol],
        previous: tuple[str, str] | None,
    ) -> tuple[str, list[str]]:
        if step.property_join is not None and step.from_ is None:
            pattern = f"({bind}:{step.entity.value})"
            clause_filters = [
                self._property_expr(bind, step.entity.value, step.property_join.local_property)
                + " = "
                + self._property_expr(
                    step.property_join.remote_variable,
                    scope[step.property_join.remote_variable].type_name,
                    step.property_join.remote_property,
                )
            ]
        elif step.from_ is None:
            pattern = f"({bind}:{step.entity.value})"
            clause_filters = []
        else:
            if step.from_.variable is not None:
                src_var = step.from_.variable
                src_type = scope[src_var].type_name
            else:
                assert previous is not None
                src_var, src_type = previous
            pattern = self._relationship_pattern(
                src_var=src_var,
                src_type=src_type,
                rel=step.from_.relationship.value,
                dst_var=bind,
                dst_type=step.entity.value,
            )
            clause_filters = []

        clause_filters.extend(
            self._compile_filter(filter_expr, scope, current_bind=bind, current_type=step.entity.value)
            for filter_expr in step.filter
        )
        clause_filters.extend(
            self._compile_not_exists(negation, scope, current_bind=bind, current_type=step.entity.value)
            for negation in step.not_exists
        )
        return pattern, clause_filters

    def _compile_not_exists(
        self,
        negation,
        scope: dict[str, _Symbol],
        *,
        current_bind: str,
        current_type: str,
    ) -> str:
        chained = self._compile_negation_chain(
            negation,
            scope,
            current_bind=current_bind,
            current_type=current_type,
        )
        if chained is not None:
            return f"NOT EXISTS {{\n  MATCH {chained}\n}}"
        local_scope = dict(scope)
        previous: tuple[str, str] | None = None
        patterns: list[str] = []
        filters: list[str] = []
        for index, step in enumerate(negation.match):
            bind = step.bind or self._fresh_var(f"n{index}")
            step_scope = dict(local_scope)
            step_scope.setdefault(current_bind, _Symbol(kind="entity", type_name=current_type))
            step_scope.setdefault(bind, _Symbol(kind="entity", type_name=step.entity.value))
            pattern, clause_filters = self._compile_match_step(
                step=step,
                bind=bind,
                scope=step_scope,
                previous=previous,
            )
            patterns.append(pattern)
            filters.extend(clause_filters)
            local_scope[bind] = _Symbol(kind="entity", type_name=step.entity.value)
            previous = (bind, step.entity.value)
        inner = f"MATCH {', '.join(patterns)}"
        if filters:
            inner += f"\n  WHERE {' AND '.join(filters)}"
        return f"NOT EXISTS {{\n  {inner}\n}}"

    def _compile_negation_chain(
        self,
        negation,
        scope: dict[str, _Symbol],
        *,
        current_bind: str,
        current_type: str,
    ) -> str | None:
        if not negation.match:
            return None
        steps = negation.match
        parts: list[str] = []
        previous_var: str | None = None
        previous_type: str | None = None
        for index, step in enumerate(steps):
            bind = step.bind
            if bind == current_bind:
                node_text = f"({current_bind}:{current_type})"
            elif bind is None:
                node_text = f"(:{step.entity.value})"
            else:
                node_text = f"({bind}:{step.entity.value})"
            if index == 0:
                if step.from_ is not None:
                    return None
                parts.append(node_text)
                previous_var = bind
                previous_type = step.entity.value
                continue
            if step.from_ is None:
                return None
            source_var = step.from_.variable
            if source_var is None:
                return None
            source_type = (
                current_type
                if source_var == current_bind
                else (previous_type if source_var == previous_var else scope.get(source_var, _Symbol("entity", "")).type_name)
            )
            if not source_type:
                return None
            rel = step.from_.relationship.value
            if source_var != previous_var:
                return None
            if self.schema.allows(rel, source_type, step.entity.value):
                parts.append(f"-[:{rel}]->{node_text}")
            elif self.schema.allows(rel, step.entity.value, source_type):
                if not parts:
                    return None
                head = parts.pop()
                parts.append(f"{node_text}-[:{rel}]->{head}")
            else:
                return None
            previous_var = bind
            previous_type = step.entity.value
        return "".join(parts)

    def _compile_stage(
        self, stage: AggregationStage, scope: dict[str, _Symbol], stage_index: int
    ) -> tuple[list[str], dict[str, _Symbol]]:
        stage_scope: dict[str, _Symbol] = {}
        with_parts: list[str] = []

        for group_key in stage.group_by:
            expr, out_name, out_symbol = self._compile_group_key(group_key, scope)
            with_parts.append(expr)
            stage_scope[out_name] = out_symbol

        for compute in stage.compute:
            expr, out_name = self._compile_compute(compute, scope)
            with_parts.append(f"{expr} AS {out_name}")
            stage_scope[out_name] = _Symbol(kind="alias", type_name="alias")

        lines = [f"WITH {', '.join(with_parts)}"]
        if stage.having:
            predicates = [
                self._compile_filter(filter_expr, stage_scope, current_bind=None, current_type=None)
                for filter_expr in stage.having
            ]
            lines.append(f"WHERE {' AND '.join(predicates)}")
        return lines, stage_scope

    def _compile_group_key(
        self, group_key: GroupKey, scope: dict[str, _Symbol]
    ) -> tuple[str, str, _Symbol]:
        if group_key.alias is not None and group_key.variable is None:
            alias = group_key.alias
            return alias, alias, scope[alias]
        assert group_key.variable is not None
        variable = group_key.variable
        symbol = scope[variable]
        if group_key.property is None:
            return variable, variable, symbol
        alias = group_key.alias
        assert alias is not None
        expr = self._property_expr(variable, symbol.type_name, group_key.property)
        return f"{expr} AS {alias}", alias, _Symbol(kind="alias", type_name="alias")

    def _compile_compute(
        self, compute: ComputeExpr, scope: dict[str, _Symbol]
    ) -> tuple[str, str]:
        alias = compute.alias or _default_compute_alias(compute, scope.get(compute.input))
        if isinstance(compute.fn, DerivedFn):
            assert compute.fn == DerivedFn.SIZE
            return f"size({compute.input})", alias

        if compute.input_property is not None:
            symbol = scope[compute.input]
            input_expr = self._property_expr(
                compute.input, symbol.type_name, compute.input_property
            )
        else:
            symbol = scope.get(compute.input)
            if symbol is not None and symbol.kind == "entity" and compute.fn in {
                AggregationFn.COLLECT,
                AggregationFn.COLLECT_DISTINCT,
            }:
                input_expr = self._property_expr(compute.input, symbol.type_name, "name")
            else:
                input_expr = compute.input

        if compute.fn == AggregationFn.COUNT:
            return f"count({compute.input})", alias
        if compute.fn == AggregationFn.COUNT_DISTINCT:
            return f"count(DISTINCT {compute.input})", alias
        if compute.fn == AggregationFn.COLLECT:
            return f"collect({input_expr})", alias
        if compute.fn == AggregationFn.COLLECT_DISTINCT:
            return f"collect(DISTINCT {input_expr})", alias
        if compute.fn == AggregationFn.SUM:
            return f"sum({input_expr})", alias
        if compute.fn == AggregationFn.SUM_MEMORY_MIB:
            return f"sum({self._memory_to_mib_expr(input_expr)})", alias
        raise ValueError(f"unsupported aggregation fn: {compute.fn}")

    def _compile_return(
        self, items: list[ReturnExpr], scope: dict[str, _Symbol], distinct: bool
    ) -> str:
        parts: list[str] = []
        for item in items:
            if item.stage_ref is not None:
                expr = item.stage_ref
                alias = item.alias or item.stage_ref
            elif item.coalesce is not None:
                refs = [self._compile_coalesce_ref(ref, scope) for ref in item.coalesce]
                expr = f"coalesce({', '.join(refs)})"
                alias = item.alias or "coalesce"
            else:
                assert item.variable is not None and item.property is not None
                expr = self._property_expr(
                    item.variable, scope[item.variable].type_name, item.property
                )
                alias = item.alias or _default_return_alias(
                    scope[item.variable].type_name, item.property
                )
            parts.append(f"{expr} AS {alias}")
        distinct_prefix = "DISTINCT " if distinct else ""
        return f"RETURN {distinct_prefix}{', '.join(parts)}"

    def _compile_coalesce_ref(self, ref: CoalesceRef, scope: dict[str, _Symbol]) -> str:
        return self._property_expr(ref.variable, scope[ref.variable].type_name, ref.property)

    def _compile_filter(
        self,
        filter_expr: FilterExpr,
        scope: dict[str, _Symbol],
        *,
        current_bind: str | None,
        current_type: str | None,
    ) -> str:
        if filter_expr.or_ is not None:
            return "(" + " OR ".join(
                self._compile_filter(item, scope, current_bind=current_bind, current_type=current_type)
                for item in filter_expr.or_
            ) + ")"
        if filter_expr.and_ is not None:
            return "(" + " AND ".join(
                self._compile_filter(item, scope, current_bind=current_bind, current_type=current_type)
                for item in filter_expr.and_
            ) + ")"
        if filter_expr.alias is not None:
            left = filter_expr.alias
        elif filter_expr.variable is not None and filter_expr.property is None:
            left = filter_expr.variable
        elif filter_expr.variable is not None and filter_expr.property is not None:
            left = self._property_expr(
                filter_expr.variable,
                scope[filter_expr.variable].type_name,
                filter_expr.property,
            )
        else:
            assert current_bind is not None and current_type is not None and filter_expr.property is not None
            left = self._property_expr(current_bind, current_type, filter_expr.property)

        if filter_expr.value_ref is not None:
            right = self._property_expr(
                filter_expr.value_ref.variable,
                scope[filter_expr.value_ref.variable].type_name,
                filter_expr.value_ref.property,
            )
            return self._binary_op(left, filter_expr.op, right, raw_rhs=True)
        return self._binary_op(left, filter_expr.op, filter_expr.value, raw_rhs=False)

    def _binary_op(
        self, left: str, op: FilterOp | None, right: object, *, raw_rhs: bool
    ) -> str:
        assert op is not None
        if op == FilterOp.IS_NULL:
            return f"{left} IS NULL"
        if op == FilterOp.IS_NOT_NULL:
            return f"{left} IS NOT NULL"
        rhs = str(right) if raw_rhs else _literal(right)
        if op == FilterOp.EQ:
            return f"{left} = {rhs}"
        if op == FilterOp.NEQ:
            return f"{left} <> {rhs}"
        if op == FilterOp.GT:
            return f"{left} > {rhs}"
        if op == FilterOp.LT:
            return f"{left} < {rhs}"
        if op == FilterOp.GTE:
            return f"{left} >= {rhs}"
        if op == FilterOp.LTE:
            return f"{left} <= {rhs}"
        if op == FilterOp.STARTS_WITH:
            return f"{left} STARTS WITH {rhs}"
        if op == FilterOp.ENDS_WITH:
            return f"{left} ENDS WITH {rhs}"
        if op == FilterOp.CONTAINS:
            return f"{left} CONTAINS {rhs}"
        raise ValueError(f"unsupported filter op: {op}")

    def _relationship_pattern(
        self, *, src_var: str, src_type: str, rel: str, dst_var: str, dst_type: str
    ) -> str:
        src_node = f"({src_var}:{src_type})"
        dst_node = f"({dst_var}:{dst_type})"
        if self.schema.allows(rel, src_type, dst_type):
            return f"{src_node}-[:{rel}]->{dst_node}"
        if self.schema.allows(rel, dst_type, src_type):
            return f"{dst_node}-[:{rel}]->{src_node}"
        raise ValueError(f"relationship {rel} does not connect {src_type} and {dst_type}")

    def _property_expr(self, variable: str, type_name: str, property_name: str) -> str:
        if type_name == EntityType.NODE.value and property_name == "name":
            return f"coalesce({variable}['metadata']['name'], {variable}['name'])"
        mapping = _ENTITY_PROPERTIES.get(type_name) or _UNWIND_PROPERTIES.get(type_name)
        if mapping is None or property_name not in mapping:
            raise ValueError(f"unknown property '{property_name}' for {type_name}")
        path = mapping[property_name]
        return _render_path(variable, path)

    def _memory_to_mib_expr(self, input_expr: str) -> str:
        return (
            "CASE "
            f"WHEN {input_expr} IS NULL OR {input_expr} = '' THEN 0 "
            f"WHEN {input_expr} ENDS WITH 'Ki' THEN toFloat(replace({input_expr}, 'Ki', '')) / 1024 "
            f"WHEN {input_expr} ENDS WITH 'Mi' THEN toFloat(replace({input_expr}, 'Mi', '')) "
            f"WHEN {input_expr} ENDS WITH 'Gi' THEN toFloat(replace({input_expr}, 'Gi', '')) * 1024 "
            f"WHEN {input_expr} ENDS WITH 'Ti' THEN toFloat(replace({input_expr}, 'Ti', '')) * 1024 * 1024 "
            f"WHEN {input_expr} ENDS WITH 'Pi' THEN toFloat(replace({input_expr}, 'Pi', '')) * 1024 * 1024 * 1024 "
            f"WHEN {input_expr} ENDS WITH 'Ei' THEN toFloat(replace({input_expr}, 'Ei', '')) * 1024 * 1024 * 1024 * 1024 "
            f"WHEN {input_expr} ENDS WITH 'K' THEN toFloat(replace({input_expr}, 'K', '')) / 1000 * 0.9765625 "
            f"WHEN {input_expr} ENDS WITH 'M' THEN toFloat(replace({input_expr}, 'M', '')) / 1000 * 976.5625 "
            f"WHEN {input_expr} ENDS WITH 'G' THEN toFloat(replace({input_expr}, 'G', '')) / 1000 * 976562.5 "
            f"WHEN {input_expr} ENDS WITH 'T' THEN toFloat(replace({input_expr}, 'T', '')) / 1000 * 976562500 "
            f"WHEN {input_expr} ENDS WITH 'P' THEN toFloat(replace({input_expr}, 'P', '')) / 1000 * 976562500000 "
            f"WHEN {input_expr} ENDS WITH 'E' THEN toFloat(replace({input_expr}, 'E', '')) / 1000 * 976562500000000 "
            f"ELSE toFloat({input_expr}) / (1024 * 1024) "
            "END"
        )

    def _fresh_var(self, prefix: str) -> str:
        self._generated += 1
        return f"_qp_{prefix}_{self._generated}"


def _render_path(variable: str, dotted_path: str) -> str:
    parts = dotted_path.split(".")
    expr = variable
    for part in parts:
        expr += f"[{json.dumps(part)}]"
    return expr


def _literal(value: object) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    escaped = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def _snake_case(value: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", value).lower()


def _default_compute_alias(compute: ComputeExpr, input_symbol: _Symbol | None) -> str:
    if compute.fn == DerivedFn.SIZE:
        return f"{compute.input}_size"
    if compute.fn == AggregationFn.SUM_MEMORY_MIB and compute.input_property is not None:
        return f"total_{compute.input_property.replace('.', '_')}_mib"
    entity_type = _snake_case(input_symbol.type_name) if input_symbol and input_symbol.type_name != "alias" else compute.input
    if compute.fn in {AggregationFn.COUNT, AggregationFn.COUNT_DISTINCT}:
        return f"{entity_type}_count"
    if compute.fn in {AggregationFn.COLLECT, AggregationFn.COLLECT_DISTINCT}:
        return f"{entity_type}_list"
    if compute.fn == AggregationFn.SUM:
        return f"total_{entity_type}"
    raise ValueError(f"unsupported compute fn for alias default: {compute.fn}")


def _default_return_alias(type_name: str, property_name: str) -> str:
    if property_name == "name":
        return _snake_case(type_name)
    return property_name.replace(".", "_")
