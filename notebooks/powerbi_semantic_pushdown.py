# Databricks notebook source
# COMMAND ----------
# COMMAND 1: Notebook documentation
# MAGIC %md
# MAGIC # Power BI to Databricks Semantic Push-Down (IMPROVED)
# MAGIC 
# MAGIC Enhanced version with better parsing and translation

# COMMAND ----------
# COMMAND 2: Pipeline configuration
config = {
    'target_catalog': 'main',
    'target_schema': 'semantic_layer',
    'measure_schema': 'semantic_measures',
    'metadata_schema': 'semantic_metadata',
    'apply_comments': True,
    'apply_tags': True,
    'create_measure_views': True,
    'create_relationships_table': True,
    'dry_run': False,
    'translate_simple_measures': True,
    'translate_time_intelligence': True,
    'log_untranslatable_measures': True,
}

config["pbi_json_path"] = (
    "/Workspace/Repos/alain.gofflot.aad@uniluxembourg.onmicrosoft.com/ANA/"
    "src/powerbi/Finance/Datasets/DS_FINANCIAL_PBIAPI_DEV.SemanticModel"
)


# COMMAND ----------
# COMMAND 3: Standard library imports
import json
import re
import os
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from collections import defaultdict, deque
import pandas as pd

# COMMAND ----------
# COMMAND 4: Data model definitions
@dataclass
class Column:
    name: str
    data_type: str
    description: str = ""
    format_string: str = ""
    is_hidden: bool = False
    is_key: bool = False
    source_column: str = ""
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class Measure:
    name: str
    expression: str
    description: str = ""
    format_string: str = ""
    display_folder: str = ""
    is_hidden: bool = False
    translated_sql: str = ""
    translation_status: str = "pending"
    translation_notes: str = ""


@dataclass
class Table:
    name: str
    description: str = ""
    is_hidden: bool = False
    columns: List[Column] = field(default_factory=list)
    measures: List[Measure] = field(default_factory=list)
    source_query: str = ""
    table_type: str = "fact"


@dataclass
class Relationship:
    from_table: str
    from_column: str
    to_table: str
    to_column: str
    cardinality: str
    cross_filter_direction: str = "single"
    is_active: bool = True


@dataclass
class SemanticModel:
    name: str
    tables: List[Table] = field(default_factory=list)
    relationships: List[Relationship] = field(default_factory=list)

    def get_table(self, name: str) -> Optional[Table]:
        return next((t for t in self.tables if t.name == name), None)


# COMMAND ----------
# COMMAND 5: DAX helper functions

def _clean_dax(s: str) -> str:
    """Clean DAX expression"""
    if not s:
        return ""
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = re.sub(r"//.*?$", "", s, flags=re.MULTILINE)
    s = re.sub(r"/\*.*?\*/", "", s, flags=re.DOTALL)
    import textwrap
    return textwrap.dedent(s).strip()


def _strip_var_return(expr: str) -> str:
    """Inline simple VAR...RETURN constructs"""
    expr = _clean_dax(expr)
    vars_found = {}

    # Extract all VAR definitions
    while True:
        m = re.match(r"^\s*VAR\s+([A-Za-z_][\w]*)\s*=\s*(.+?)\s*(?:\n|$)",
                     expr, flags=re.IGNORECASE | re.DOTALL)
        if not m:
            break
        name = m.group(1).strip()
        val = m.group(2).strip()
        vars_found[name] = val
        expr = expr[m.end():].lstrip()

    # Extract RETURN clause
    mret = re.match(r"^\s*RETURN\s+(.+)$", expr, flags=re.IGNORECASE | re.DOTALL)
    if mret:
        expr = mret.group(1).strip()

    # Substitute variables
    for k, v in vars_found.items():
        expr = re.sub(rf"\b{re.escape(k)}\b", f"({v})", expr)

    return expr


def _measure_refs(expr: str) -> List[str]:
    """Extract measure references from expression"""
    return re.findall(r"\[([^\]]+)\]", expr or "")


def _split_top_commas(inner: str) -> List[str]:
    """Split by commas at top level (respecting parentheses)"""
    parts, buf, depth = [], [], 0
    for ch in inner:
        if ch == '(':
            depth += 1
        elif ch == ')':
            depth -= 1
        if ch == ',' and depth == 0:
            parts.append(''.join(buf).strip())
            buf = []
        else:
            buf.append(ch)
    tail = ''.join(buf).strip()
    if tail:
        parts.append(tail)
    return parts


# COMMAND ----------
# COMMAND 6: Relationship graph utilities
class RelGraph:
    def __init__(self):
        self.adj = defaultdict(list)

    def add(self, a_tbl, a_col, b_tbl, b_col):
        self.adj[a_tbl].append((b_tbl, a_col, b_col))
        self.adj[b_tbl].append((a_tbl, b_col, a_col))

    def path(self, src, dst):
        """Find shortest path between tables"""
        if src == dst:
            return []
        seen, q = {src}, deque([(src, [])])
        while q:
            cur, path = q.popleft()
            for nxt, lcol, rcol in self.adj[cur]:
                if nxt in seen:
                    continue
                np = path + [(cur, lcol, nxt, rcol)]
                if nxt == dst:
                    return np
                seen.add(nxt)
                q.append((nxt, np))
        return None


# COMMAND ----------
# COMMAND 7: Power BI model parser
class PowerBIParser:
    def __init__(self, source_path: str):
        self.source_path = source_path
        self.raw_data = None
        self.is_tmdl = False

    def parse_model(self) -> SemanticModel:
        """Load and translate the source model into dataclasses."""
        raw = self.load_json()
        model_root = raw.get('model', {})
        tables_data = model_root.get('tables', [])
        relationships_data = model_root.get('relationships', [])

        tables: List[Table] = []
        for table_entry in tables_data:
            table = Table(
                name=table_entry.get('name', ''),
                description=table_entry.get('description', ''),
                is_hidden=table_entry.get('isHidden', False),
                source_query=table_entry.get('source', {}).get('expression', '')
                if isinstance(table_entry.get('source'), dict)
                else table_entry.get('sourceQuery', ''),
                table_type=table_entry.get('tableType') or table_entry.get('type', 'fact') or 'fact'
            )

            for column_entry in table_entry.get('columns', []):
                column = Column(
                    name=column_entry.get('name', ''),
                    data_type=column_entry.get('dataType', 'string'),
                    description=column_entry.get('description', ''),
                    format_string=column_entry.get('formatString', ''),
                    is_hidden=column_entry.get('isHidden', False),
                    is_key=column_entry.get('isKey', False),
                    source_column=column_entry.get('sourceColumn', '')
                )
                annotations = column_entry.get('annotations', []) or []
                if isinstance(annotations, list):
                    for ann in annotations:
                        if isinstance(ann, dict) and ann.get('name') and ann.get('value'):
                            column.tags[ann['name']] = ann['value']
                elif isinstance(annotations, dict):
                    column.tags.update({str(k): str(v) for k, v in annotations.items()})
                table.columns.append(column)

            for measure_entry in table_entry.get('measures', []):
                measure = Measure(
                    name=measure_entry.get('name', ''),
                    expression=measure_entry.get('expression', ''),
                    description=measure_entry.get('description', ''),
                    format_string=measure_entry.get('formatString', ''),
                    display_folder=measure_entry.get('displayFolder', ''),
                    is_hidden=measure_entry.get('isHidden', False)
                )
                table.measures.append(measure)

            tables.append(table)

        relationships: List[Relationship] = []
        for rel_entry in relationships_data:
            try:
                relationships.append(
                    Relationship(
                        from_table=rel_entry.get('fromTable', ''),
                        from_column=rel_entry.get('fromColumn', ''),
                        to_table=rel_entry.get('toTable', ''),
                        to_column=rel_entry.get('toColumn', ''),
                        cardinality=rel_entry.get('cardinality', 'manyToOne'),
                        cross_filter_direction=rel_entry.get('crossFilteringBehavior', rel_entry.get('crossFilterDirection', 'single')),
                        is_active=rel_entry.get('isActive', True)
                    )
                )
            except Exception:
                continue

        model_name = raw.get('name') or model_root.get('name') or os.path.basename(self.source_path)
        return SemanticModel(name=model_name, tables=tables, relationships=relationships)

    def _check_format(self) -> str:
        """Determine if source is JSON or TMDL"""
        if os.path.isdir(self.source_path):
            definition_folder = os.path.join(self.source_path, 'definition')
            if os.path.exists(definition_folder) and os.path.isdir(definition_folder):
                self.is_tmdl = True
                return 'tmdl'
            for file in ['model.bim', 'definition.pbism']:
                if os.path.exists(os.path.join(self.source_path, file)):
                    return 'json'
        elif os.path.isfile(self.source_path):
            if self.source_path.endswith('.tmdl'):
                self.is_tmdl = True
                return 'tmdl'
            return 'json'
        raise ValueError(f"Could not determine format for: {self.source_path}")

    def load_json(self) -> Dict:
        """Load Power BI model"""
        try:
            format_type = self._check_format()
            print(f"✓ Detected format: {format_type.upper()}")
            if format_type == 'tmdl':
                self.raw_data = self._parse_tmdl_folder()
            else:
                self.raw_data = self._parse_json_file()
            print(f"✓ Successfully loaded model from {self.source_path}")
            return self.raw_data
        except Exception as e:
            print(f"✗ Error loading model: {str(e)}")
            import traceback
            traceback.print_exc()
            raise

    def _parse_json_file(self) -> Dict:
        """Parse JSON format"""
        json_file = self.source_path
        if os.path.isdir(self.source_path):
            for filename in ['model.bim', 'definition.pbism', 'database.json']:
                candidate = os.path.join(self.source_path, filename)
                if os.path.exists(candidate):
                    json_file = candidate
                    break
        with open(json_file, 'r', encoding='utf-8-sig') as f:
            return json.load(f)

    def _parse_tmdl_folder(self) -> Dict:
        """Parse TMDL format"""
        definition_folder = os.path.join(self.source_path, 'definition')
        if not os.path.exists(definition_folder):
            definition_folder = self.source_path
        print(f"  Parsing TMDL from: {definition_folder}")

        model_data = {
            'name': os.path.basename(self.source_path),
            'model': {'tables': [], 'relationships': []}
        }

        # Parse database info
        database_file = os.path.join(definition_folder, 'database.tmdl')
        if os.path.exists(database_file):
            database_info = self._parse_tmdl_file(database_file)
            if database_info and 'name' in database_info:
                model_data['name'] = database_info['name']

        # Parse tables
        tables_folder = os.path.join(definition_folder, 'tables')
        if os.path.exists(tables_folder):
            for table_file in sorted(os.listdir(tables_folder)):
                if table_file.endswith('.tmdl'):
                    table_path = os.path.join(tables_folder, table_file)
                    table_data = self._parse_table_tmdl(table_path)
                    if table_data:
                        model_data['model']['tables'].append(table_data)
            print(f"  ✓ Parsed {len(model_data['model']['tables'])} tables")

        # Parse relationships
        relationships_file = os.path.join(definition_folder, 'relationships.tmdl')
        if os.path.exists(relationships_file):
            relationships = self._parse_relationships_tmdl(relationships_file)
            model_data['model']['relationships'] = relationships
            print(f"  ✓ Parsed {len(relationships)} relationships")

        return model_data

    def _parse_tmdl_file(self, file_path: str) -> Dict:
        """Parse single TMDL file"""
        try:
            with open(file_path, 'r', encoding='utf-8-sig') as f:
                content = f.read()
            data = {}
            name_match = re.search(r'name:\s*["\']?([^"\'\n]+)["\']?', content, re.IGNORECASE)
            if name_match:
                data['name'] = name_match.group(1).strip()
            return data
        except Exception as e:
            print(f"  ⚠ Warning: Could not parse {file_path}: {str(e)}")
            return {}

    def _parse_table_tmdl(self, table_path: str) -> Dict:
        """Parse table TMDL with robust multi-line DAX extraction"""
        try:
            with open(table_path, 'r', encoding='utf-8-sig') as f:
                content = f.read()

            table_name = os.path.basename(table_path).replace('.tmdl', '')
            table_data = {
                'name': table_name,
                'columns': [],
                'measures': []
            }

            # Description
            desc_match = re.search(r'description:\s*"""(.*?)"""', content, re.DOTALL)
            if not desc_match:
                desc_match = re.search(r'description:\s*"([^"]*)"', content)
            if desc_match:
                table_data['description'] = desc_match.group(1).strip()

            # Hidden
            if re.search(r'isHidden:\s*true', content, re.IGNORECASE):
                table_data['isHidden'] = True

            # Columns
            column_pattern = r'column\s+([^\s]+)(?:\s*=\s*(.+?))?(?:\n\s*{(.*?)\n\s*})?'
            for match in re.finditer(column_pattern, content, re.DOTALL):
                col_name = match.group(1).strip("'\"")
                col_props = match.group(3) if match.group(3) else ""
                column = {'name': col_name, 'dataType': 'string'}

                if col_props:
                    dtype_match = re.search(r'dataType:\s*(\w+)', col_props)
                    if dtype_match:
                        column['dataType'] = dtype_match.group(1)
                    d = re.search(r'description:\s*"""(.*?)"""', col_props, re.DOTALL) or \
                        re.search(r'description:\s*"([^"]*)"', col_props)
                    if d:
                        column['description'] = d.group(1).strip()
                    fstr = re.search(r'formatString:\s*"([^"]*)"', col_props)
                    if fstr:
                        column['formatString'] = fstr.group(1)
                    src = re.search(r'sourceColumn:\s*"([^"]*)"', col_props)
                    if src:
                        column['sourceColumn'] = src.group(1)
                    if re.search(r'isHidden:\s*true', col_props, re.IGNORECASE):
                        column['isHidden'] = True
                    if re.search(r'isKey:\s*true', col_props, re.IGNORECASE):
                        column['isKey'] = True

                table_data['columns'].append(column)

            # Helper to extract DAX expression starting at index
            def _extract_dax_expression(buf: str, start_idx: int):
                i = start_idx
                n = len(buf)

                # Fenced code block
                if buf[i:i + 3] == "```":
                    i += 3
                    while i < n and buf[i] != '\n':
                        i += 1
                    i += 1  # move past language line break
                    body_start = i
                    fence_pos = buf.find("```", body_start)
                    if fence_pos == -1:
                        fence_pos = n
                    return buf[body_start:fence_pos].strip(), fence_pos + 3

                # Plain expression, track parens and quotes
                depth = 0
                in_quote = None
                prev = ''
                body_start = i
                while i < n:
                    ch = buf[i]
                    if in_quote:
                        if ch == in_quote and prev != '\\':
                            in_quote = None
                        i += 1
                        prev = ch
                        continue
                    if ch in ("'", '"'):
                        in_quote = ch
                    elif ch == '(':
                        depth += 1
                    elif ch == ')':
                        depth = max(0, depth - 1)

                    if ch == '\n':
                        look = buf[i + 1:i + 1 + 60]
                        if depth == 0 and re.match(r'\s*(description:|formatString:|displayFolder:|isHidden:|measure\s+)', look):
                            break
                    i += 1
                    prev = ch
                return buf[body_start:i].strip(), i

            # Measures using header scan and expression extraction
            measure_header = re.compile(r'^\s*measure\s+(?:"([^"]+)"|\'([^\']+)\'|([^\s=]+))\s*=\s*', re.MULTILINE)
            measures = []
            for m in measure_header.finditer(content):
                name = next(g for g in m.groups()[:3] if g is not None).strip()
                expr, end_idx = _extract_dax_expression(content, m.end())
                next_m = measure_header.search(content, end_idx)
                props = content[end_idx: next_m.start()] if next_m else content[end_idx:]
                meas = {'name': name, 'expression': expr}

                d = re.search(r'description:\s*"""(.*?)"""', props, re.DOTALL) or re.search(r'description:\s*"([^"]*)"', props)
                if d:
                    meas['description'] = d.group(1).strip()
                f = re.search(r'formatString:\s*"([^"]*)"', props)
                if f:
                    meas['formatString'] = f.group(1)
                folder = re.search(r'displayFolder:\s*"([^"]*)"', props)
                if folder:
                    meas['displayFolder'] = folder.group(1)
                if re.search(r'isHidden:\s*true', props, re.IGNORECASE):
                    meas['isHidden'] = True

                measures.append(meas)

            table_data['measures'].extend(measures)
            print(f"  ✓ Parsed '{table_name}': {len(table_data['columns'])} columns, {len(table_data['measures'])} measures")
            return table_data
        except Exception as e:
            print(f"  ✗ Error parsing table {table_path}: {str(e)}")
            import traceback
            traceback.print_exc()
            return None

    def _parse_relationships_tmdl(self, rel_path: str) -> List[Dict]:
        """Parse relationships TMDL, supports quoted table names and bracketed columns"""

        def _parse_rel_ref(txt: str):
            m = re.search(
                r"""\s*
                (?:'([^']+)'|"([^"]+)"|([^\.\s]+))    # table, possibly quoted
                \s*\.\s*
                \[\s*([^\]]+)\s*\]                    # column in brackets
            """, txt, re.VERBOSE)
            if not m:
                return None
            table = next(g for g in m.groups()[:3] if g)
            col = m.group(4)
            return table, col

        try:
            with open(rel_path, 'r', encoding='utf-8-sig') as f:
                content = f.read()
            relationships = []
            for match in re.finditer(r'relationship\s+"?([^"\n]+)"?\s*{(.*?)}', content, re.DOTALL):
                props = match.group(2)
                fr = re.search(r'fromColumn:\s*(.+)', props)
                to = re.search(r'toColumn:\s*(.+)', props)
                if not fr or not to:
                    continue
                fr_ref = _parse_rel_ref(fr.group(1))
                to_ref = _parse_rel_ref(to.group(1))
                if not fr_ref or not to_ref:
                    continue

                card = re.search(r'cardinality:\s*(\w+)', props)
                cross = re.search(r'crossFilteringBehavior:\s*(\w+)', props)
                is_active = not re.search(r'isActive:\s*false', props, re.IGNORECASE)

                relationships.append({
                    'fromTable': fr_ref[0],
                    'fromColumn': fr_ref[1],
                    'toTable': to_ref[0],
                    'toColumn': to_ref[1],
                    'cardinality': card.group(1) if card else 'manyToOne',
                    'crossFilteringBehavior': cross.group(1) if cross else 'single',
                    'isActive': is_active
                })
            print(f"  ✓ Parsed {len(relationships)} relationships")
            return relationships
        except Exception as e:
            print(f"  ✗ Error parsing relationships: {str(e)}")
            return []


# COMMAND ----------
# COMMAND 8: DAX translator
class DAXTranslator:
    def __init__(self, relationships: List[Relationship] = None):
        self.rel_graph = RelGraph()
        if relationships:
            for r in relationships:
                self.rel_graph.add(r.from_table, r.from_column, r.to_table, r.to_column)

        # Regex patterns
        self.SUM_RE = re.compile(
            r"""^\s*SUM\s*\(\s*'?(?P<table>[^'\]]+)'?\s*\[\s*(?P<col>[^\]]+)\s*\]\s*\)\s*$""",
            re.IGNORECASE
        )
        self.COL_RE = re.compile(r"""'?(?P<table>[^'\]]+)'?\s*\[\s*(?P<col>[^\]]+)\s*\]""")
        # Allow only arithmetic characters when combining translated measures.
        # Place the hyphen at the end of the character class to avoid creating
        # an accidental range such as "- *", which raised a runtime regex
        # compilation error on Databricks runtimes.
        self.SAFE_TOKENS_RE = re.compile(r"^[0-9.\s+*/(),-]*$", re.IGNORECASE)

    def translate_all_measures(self, model: SemanticModel) -> Dict[str, Any]:
        """Translate all measures with dependency resolution"""
        all_measures = []
        measure_to_table = {}

        # Collect all measures
        for table in model.tables:
            for measure in table.measures:
                measure.expression = _strip_var_return(_clean_dax(measure.expression))
                all_measures.append(measure)
                measure_to_table[measure.name] = table.name

        # Build dependency graph
        deps = {m.name: [x for x in _measure_refs(m.expression) if x != m.name]
                for m in all_measures}
        indeg = {m.name: sum(1 for v in deps[m.name] if v in deps) for m in all_measures}

        # Topological sort
        queue = deque([m for m in all_measures if indeg[m.name] == 0])
        seen = set()
        produced = {}

        while queue:
            measure = queue.popleft()
            seen.add(measure.name)

            # Try translation strategies in order
            sql, status = self._translate_calculate(measure.expression, measure_to_table.get(measure.name))
            if status != "success":
                sql, status = self._translate_algebra(measure.expression, produced)
            if status != "success":
                sql, status = self._translate_simple(measure.expression, measure_to_table.get(measure.name))

            if status == "success" and sql:
                produced[measure.name] = sql
                measure.translated_sql = sql
                measure.translation_status = "success"
            else:
                measure.translation_status = "manual_required"
                measure.translation_notes = self._generate_notes(measure.expression)

            # Update dependencies
            for k, vs in deps.items():
                if measure.name in vs:
                    indeg[k] -= 1
                    if indeg[k] == 0 and k not in seen:
                        nxt = next((x for x in all_measures if x.name == k), None)
                        if nxt:
                            queue.append(nxt)

        # Calculate statistics
        auto = [m for m in all_measures if m.translation_status == "success"]
        manual = [m for m in all_measures if m.translation_status == "manual_required"]

        return {
            'total': len(all_measures),
            'success': len(auto),
            'manual_required': len(manual),
            'pending': len(all_measures) - len(auto) - len(manual)
        }

    def _translate_simple(self, expr: str, table_name: str = None) -> Tuple[str, str]:
        """Translate simple aggregation measures"""
        # SUM(Table[Column])
        m = self.SUM_RE.match(expr)
        if m:
            tbl, col = m.group("table").strip(), m.group("col").strip()
            sql = f"SELECT SUM(`{col}`) AS val FROM `{tbl}`"
            return sql, "success"

        # COUNTROWS(Table)
        m = re.match(r"^\s*COUNTROWS\s*\(\s*'?([^'\)]+)'?\s*\)\s*$", expr, re.IGNORECASE)
        if m:
            tbl = m.group(1).strip()
            sql = f"SELECT COUNT(*) AS val FROM `{tbl}`"
            return sql, "success"

        # DISTINCTCOUNT(Table[Column])
        m = re.match(r"^\s*DISTINCTCOUNT\s*\(\s*'?([^'\]]+)'?\s*\[\s*([^\]]+)\s*\]\s*\)\s*$",
                     expr, re.IGNORECASE)
        if m:
            tbl, col = m.group(1).strip(), m.group(2).strip()
            sql = f"SELECT COUNT(DISTINCT `{col}`) AS val FROM `{tbl}`"
            return sql, "success"

        # AVERAGE(Table[Column])
        m = re.match(r"^\s*AVERAGE\s*\(\s*'?([^'\]]+)'?\s*\[\s*([^\]]+)\s*\]\s*\)\s*$",
                     expr, re.IGNORECASE)
        if m:
            tbl, col = m.group(1).strip(), m.group(2).strip()
            sql = f"SELECT AVG(`{col}`) AS val FROM `{tbl}`"
            return sql, "success"

        return None, "not_applicable"

    def _translate_calculate(self, expr: str, table_name: str = None) -> Tuple[str, str]:
        """Translate CALCULATE expressions"""
        m = re.match(r"^\s*CALCULATE\s*\((.*)\)\s*$", expr, flags=re.IGNORECASE | re.DOTALL)
        if not m:
            return None, "not_applicable"

        inner = m.group(1).strip()
        args = _split_top_commas(inner)
        if not args:
            return None, "manual_required"

        # First arg should be aggregation
        agg = args[0].strip()
        sm = self.SUM_RE.match(agg)
        if not sm:
            return None, "manual_required"

        base_tbl, agg_col = sm.group("table").strip(), sm.group("col").strip()

        # Parse filter conditions
        conds = []
        for a in args[1:]:
            parsed = self._parse_filter_arg(a)
            if parsed:
                conds.extend(parsed)

        # Build SQL
        from_clause, where_clause = self._joins_and_where(base_tbl, conds)
        sql = f"SELECT SUM(`f0`.`{agg_col}`) AS val FROM {from_clause}{where_clause}"
        return sql, "success"

    def _translate_algebra(self, expr: str, produced_sql: Dict[str, str]) -> Tuple[str, str]:
        """Translate arithmetic expressions combining measures"""
        expr = expr.strip()
        refs = set(_measure_refs(expr))
        if not refs:
            return None, "not_applicable"

        # Check all referenced measures are available
        for r in refs:
            if r not in produced_sql:
                return None, "manual_required"

        # Replace measure refs with SQL subqueries
        rep = expr
        for r in refs:
            rep = rep.replace(f"[{r}]", f"({produced_sql[r]})")

        # Allow COALESCE wrappers
        rep = re.sub(r"COALESCE\s*\(\s*\((SELECT .*?)\)\s*,\s*0\s*\)",
                     r"COALESCE(\1, 0)", rep, flags=re.IGNORECASE | re.DOTALL)

        # Verify remaining tokens are safe arithmetic
        leftover = re.sub(r"\(SELECT .*?\)", "", rep, flags=re.DOTALL)
        if not self.SAFE_TOKENS_RE.match(leftover):
            return None, "manual_required"

        return rep, "success"

    def _parse_filter_arg(self, arg: str) -> Optional[List]:
        """Parse filter argument into conditions"""
        arg = arg.strip()
        p = self._parse_simple_cond(arg)
        if p:
            return [p]

        # FILTER(Table, condition)
        m = re.match(r"^\s*FILTER\s*\(\s*'?(?P<table>[^'\)]*)'?\s*,\s*(?P<cond>.+)\)\s*$",
                     arg, flags=re.IGNORECASE | re.DOTALL)
        if m:
            pc = self._parse_simple_cond(m.group("cond"))
            if pc:
                kind, _, col, *rest = pc
                table = m.group("table").strip()
                if kind == "cmp":
                    return [("cmp", table, col, rest[0], rest[1])]
                if kind == "in":
                    return [("in", table, col, rest[0])]

        return None

    def _parse_simple_cond(self, cond: str) -> Optional[Tuple]:
        """Parse simple condition"""
        c = cond.strip()
        # Strip VALUE() wrapper
        c = re.sub(r"^\s*VALUE\s*\(\s*", "", c, flags=re.IGNORECASE)
        c = re.sub(r"\)\s*$", "", c)

        # Comparison: Table[Column] = value
        m = re.match(rf"^{self.COL_RE.pattern}\s*(=|==|<>|!=|>|<|>=|<=)\s*(.+)$",
                     c, flags=re.IGNORECASE)
        if m:
            op = m.group(3)
            rhs = m.group(4).strip()
            if op == "==":
                op = "="
            if op == "!=":
                op = "<>"
            # Check RHS is literal
            if re.match(r"""^"[^"]*"|'[^']*'|[0-9]+(\.[0-9]+)?$""", rhs.strip()):
                return ("cmp", m.group("table"), m.group("col"), op, rhs)

        # IN operator: Table[Column] IN {values}
        m = re.match(rf"^{self.COL_RE.pattern}\s+IN\s*\{{(.+)\}}\s*$",
                     c, flags=re.IGNORECASE)
        if m:
            items = [x.strip() for x in m.group(3).split(",") if x.strip()]
            safe = all(re.match(r"""^"[^"]*"|'[^']*'|[0-9]+(\.[0-9]+)?$""", it)
                        for it in items)
            if safe:
                return ("in", m.group("table"), m.group("col"), items)

        return None

    def _joins_and_where(self, base_tbl: str, conds: List) -> Tuple[str, str]:
        """Build FROM clause with JOINs and WHERE clause"""
        from_sql = f"`{base_tbl}` `f0`"
        alias = {base_tbl: "f0"}
        join_seq = []

        # Determine needed joins
        for c in conds:
            tbl = c[1]
            if tbl == base_tbl:
                continue
            path = self.rel_graph.path(base_tbl, tbl)
            if path:
                join_seq.extend(path)

        # Build JOIN clauses
        jidx = 1
        for a_tbl, a_col, b_tbl, b_col in join_seq:
            if b_tbl in alias:
                continue
            if a_tbl not in alias:
                alias[a_tbl] = f"f{jidx}"
                jidx += 1
            alias[b_tbl] = f"f{jidx}"
            jidx += 1
            left = f"`{alias[a_tbl]}`.`{a_col}`"
            right = f"`{alias[b_tbl]}`.`{b_col}`"
            from_sql += f" JOIN `{b_tbl}` `{alias[b_tbl]}` ON {left} = {right}"

        # Build WHERE clause
        where = []
        for c in conds:
            kind, tbl, col = c[0], c[1], c[2]
            if tbl not in alias and tbl != base_tbl:
                continue
            a = alias.get(tbl, "f0")
            if kind == "cmp":
                where.append(f"`{a}`.`{col}` {c[3]} {c[4]}")
            elif kind == "in":
                items = ", ".join(c[3])
                where.append(f"`{a}`.`{col}` IN ({items})")

        wsql = (" WHERE " + " AND ".join(where)) if where else ""
        return from_sql, wsql

    def _generate_notes(self, expr: str) -> str:
        """Generate helpful notes for manual translation"""
        notes = []

        if re.search(r'\bCALCULATE\b', expr, re.IGNORECASE):
            notes.append("Contains CALCULATE - may need complex filter logic")
        if re.search(r'\bFILTER\b', expr, re.IGNORECASE):
            notes.append("Contains FILTER - requires row-level filtering")
        if re.search(r'\bALL\b|\bALLEXCEPT\b', expr, re.IGNORECASE):
            notes.append("Contains ALL/ALLEXCEPT - context modification needed")
        if re.search(r'\bDATE\w+\b', expr, re.IGNORECASE):
            notes.append("Contains time intelligence - may need date dimension")
        if re.search(r'\bRELATED\b', expr, re.IGNORECASE):
            notes.append("Contains RELATED - needs relationship traversal")
        if re.search(r'\bIF\b', expr, re.IGNORECASE):
            notes.append("Contains IF - use CASE WHEN in SQL")
        if re.search(r'\bSWITCH\b', expr, re.IGNORECASE):
            notes.append("Contains SWITCH - use CASE WHEN in SQL")

        return "; ".join(notes) if notes else "Complex DAX pattern"


# COMMAND ----------
# COMMAND 9: Unity Catalog applicator
class UnityCatalogApplicator:
    def __init__(self, catalog: str, schema: str, dry_run: bool = False):
        self.catalog = catalog
        self.schema = schema
        self.dry_run = dry_run
        self.sql_statements = []

    def apply_table_comments(self, table: Table) -> List[str]:
        statements = []
        if table.description:
            full_table_name = f"{self.catalog}.{self.schema}.{table.name}"
            comment_sql = f"COMMENT ON TABLE {full_table_name} IS '{self._escape_sql_string(table.description)}'"
            statements.append(comment_sql.strip())

            if not self.dry_run:
                try:
                    spark.sql(comment_sql)
                    print(f"✓ Applied comment to table {table.name}")
                except Exception as e:
                    print(f"✗ Error applying comment to {table.name}: {str(e)}")

        return statements

    def apply_column_comments(self, table: Table) -> List[str]:
        statements = []
        full_table_name = f"{self.catalog}.{self.schema}.{table.name}"

        for column in table.columns:
            if column.description:
                comment_sql = f"ALTER TABLE {full_table_name} ALTER COLUMN {column.name} COMMENT '{self._escape_sql_string(column.description)}'"
                statements.append(comment_sql.strip())

                if not self.dry_run:
                    try:
                        spark.sql(comment_sql)
                        print(f"  ✓ Applied comment to {table.name}.{column.name}")
                    except Exception as e:
                        print(f"  ✗ Error applying comment to {table.name}.{column.name}: {str(e)}")

        return statements

    def apply_table_tags(self, table: Table) -> List[str]:
        statements = []
        full_table_name = f"{self.catalog}.{self.schema}.{table.name}"

        tags = {
            'table_type': table.table_type,
            'source': 'power_bi',
            'last_updated': datetime.now().isoformat()
        }

        tag_pairs = [f"'{k}' = '{v}'" for k, v in tags.items()]
        tag_sql = f"ALTER TABLE {full_table_name} SET TAGS ({', '.join(tag_pairs)})"
        statements.append(tag_sql.strip())

        if not self.dry_run:
            try:
                spark.sql(tag_sql)
                print(f"✓ Applied tags to table {table.name}")
            except Exception as e:
                print(f"✗ Error applying tags to {table.name}: {str(e)}")

        return statements

    def apply_column_tags(self, table: Table) -> List[str]:
        statements = []
        full_table_name = f"{self.catalog}.{self.schema}.{table.name}"

        for column in table.columns:
            if column.tags:
                tag_pairs = [f"'{k}' = '{v}'" for k, v in column.tags.items()]
                tag_sql = f"ALTER TABLE {full_table_name} ALTER COLUMN {column.name} SET TAGS ({', '.join(tag_pairs)})"
                statements.append(tag_sql.strip())

                if not self.dry_run:
                    try:
                        spark.sql(tag_sql)
                        print(f"  ✓ Applied tags to {table.name}.{column.name}")
                    except Exception as e:
                        print(f"  ✗ Error applying tags to {table.name}.{column.name}: {str(e)}")

        return statements

    def _escape_sql_string(self, text: str) -> str:
        return text.replace("'", "''")


# COMMAND ----------
# COMMAND 10: Measure view generator
class MeasureViewGenerator:
    def __init__(self, catalog: str, source_schema: str, measure_schema: str, dry_run: bool = False):
        self.catalog = catalog
        self.source_schema = source_schema
        self.measure_schema = measure_schema
        self.dry_run = dry_run

        if not dry_run:
            try:
                spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{measure_schema}")
                print(f"✓ Ensured schema exists: {catalog}.{measure_schema}")
            except Exception as e:
                print(f"✗ Error creating schema: {str(e)}")

    def create_measure_view(self, table: Table, measure: Measure) -> Optional[str]:
        if measure.translation_status != 'success' or not measure.translated_sql:
            return None

        view_name = f"{measure.name.replace(' ', '_').lower()}"
        full_view_name = f"{self.catalog}.{self.measure_schema}.{view_name}"
        source_table = f"{self.catalog}.{self.source_schema}.{table.name}"

        view_sql = f"""
        CREATE OR REPLACE VIEW {full_view_name}
        COMMENT '{self._escape_sql_string(measure.description or measure.name)}'
        AS
        {measure.translated_sql}
        """

        if not self.dry_run:
            try:
                spark.sql(view_sql)
                print(f"  ✓ Created view: {view_name}")
            except Exception as e:
                print(f"  ✗ Error creating view {view_name}: {str(e)}")
                return None

        return view_sql

    def create_all_measure_views(self, model: SemanticModel) -> List[str]:
        view_statements = []
        for table in model.tables:
            for measure in table.measures:
                view_sql = self.create_measure_view(table, measure)
                if view_sql:
                    view_statements.append(view_sql)
        return view_statements

    def _escape_sql_string(self, text: str) -> str:
        return text.replace("'", "''")


# COMMAND ----------
# COMMAND 11: Relationship registry
class RelationshipRegistry:
    def __init__(self, catalog: str, metadata_schema: str, dry_run: bool = False):
        self.catalog = catalog
        self.metadata_schema = metadata_schema
        self.dry_run = dry_run
        self.table_name = f"{catalog}.{metadata_schema}.relationships"

        if not dry_run:
            self._ensure_schema_exists()
            self._create_relationships_table()

    def _ensure_schema_exists(self):
        try:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.metadata_schema}")
            print(f"✓ Ensured schema exists: {self.catalog}.{self.metadata_schema}")
        except Exception as e:
            print(f"✗ Error creating schema: {str(e)}")

    def _create_relationships_table(self):
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            relationship_id STRING,
            from_table STRING,
            from_column STRING,
            to_table STRING,
            to_column STRING,
            cardinality STRING,
            cross_filter_direction STRING,
            is_active BOOLEAN,
            created_at TIMESTAMP,
            source STRING
        )
        COMMENT 'Metadata table storing relationships from Power BI semantic model'
        """

        try:
            spark.sql(create_sql)
            print(f"✓ Relationships table ready: {self.table_name}")
        except Exception as e:
            print(f"✗ Error creating relationships table: {str(e)}")

    def register_relationships(self, relationships: List[Relationship]) -> int:
        if self.dry_run:
            print(f"[DRY RUN] Would register {len(relationships)} relationships")
            return len(relationships)

        data = []
        for rel in relationships:
            rel_id = f"{rel.from_table}.{rel.from_column}_to_{rel.to_table}.{rel.to_column}"
            data.append({
                'relationship_id': rel_id,
                'from_table': rel.from_table,
                'from_column': rel.from_column,
                'to_table': rel.to_table,
                'to_column': rel.to_column,
                'cardinality': rel.cardinality,
                'cross_filter_direction': rel.cross_filter_direction,
                'is_active': rel.is_active,
                'created_at': datetime.now(),
                'source': 'power_bi'
            })

        if data:
            df = spark.createDataFrame(data)
            try:
                spark.sql(f"DELETE FROM {self.table_name} WHERE source = 'power_bi'")
                df.write.mode('append').saveAsTable(self.table_name)
                print(f"✓ Registered {len(data)} relationships")
                return len(data)
            except Exception as e:
                print(f"✗ Error registering relationships: {str(e)}")
                return 0

        return 0


# COMMAND ----------
# COMMAND 12: Documentation generator
class DocumentationGenerator:
    def __init__(self, model: SemanticModel):
        self.model = model

    def display_summary(self):
        stats = self.generate_summary_stats()
        print(f"Model: {self.model.name}")
        print(f"Tables: {stats['tables']}")
        print(f"Columns: {stats['columns']}")
        print(f"Measures: {stats['measures']}")
        print(f"Relationships: {stats['relationships']}")

    def generate_summary_stats(self) -> Dict[str, int]:
        return {
            "tables": len(self.model.tables),
            "columns": sum(len(t.columns) for t in self.model.tables),
            "measures": sum(len(t.measures) for t in self.model.tables),
            "relationships": len(self.model.relationships),
        }

    def generate_tables_dataframe(self) -> pd.DataFrame:
        rows = []
        for t in self.model.tables:
            rows.append({
                "Table": t.name,
                "Type": t.table_type,
                "Columns": len(t.columns),
                "Measures": len(t.measures),
                "Hidden": t.is_hidden,
                "Description": t.description,
            })
        return pd.DataFrame(rows).sort_values(["Type", "Table"]).reset_index(drop=True)

    def generate_measures_dataframe(self) -> pd.DataFrame:
        rows = []
        for t in self.model.tables:
            for m in t.measures:
                rows.append({
                    "Table": t.name,
                    "Measure": m.name,
                    "DAX": m.expression,
                    "Translated SQL": m.translated_sql,
                    "Status": m.translation_status,
                    "Notes": m.translation_notes,
                    "Display Folder": m.display_folder,
                    "Hidden": m.is_hidden,
                    "Format": m.format_string,
                    "Description": m.description,
                })
        df = pd.DataFrame(rows) if rows else pd.DataFrame(
            columns=["Table", "Measure", "DAX", "Translated SQL", "Status", "Notes", "Display Folder", "Hidden", "Format", "Description"]
        )
        return df.sort_values(["Table", "Measure"]).reset_index(drop=True)

    def generate_relationships_dataframe(self) -> pd.DataFrame:
        rows = []
        for r in self.model.relationships:
            rows.append({
                "From Table": r.from_table,
                "From Column": r.from_column,
                "To Table": r.to_table,
                "To Column": r.to_column,
                "Cardinality": r.cardinality,
                "Cross Filter": r.cross_filter_direction,
                "Active": r.is_active,
            })
        df = pd.DataFrame(rows) if rows else pd.DataFrame(
            columns=["From Table", "From Column", "To Table", "To Column", "Cardinality", "Cross Filter", "Active"]
        )
        return df.sort_values(["From Table", "To Table"]).reset_index(drop=True)


# COMMAND ----------
# COMMAND 13: Orchestrator
class SemanticPushDownOrchestrator:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.model = None
        self.parser = None
        self.translator = None
        self.applicator = None
        self.measure_generator = None
        self.relationship_registry = None
        self.doc_generator = None

    def run(self) -> Dict[str, Any]:
        results = {
            'success': False,
            'steps_completed': [],
            'errors': [],
            'stats': {}
        }

        try:
            # Step 1: Parse
            print("\n" + "=" * 80)
            print("STEP 1: Parsing Power BI Model")
            print("=" * 80)
            self.parser = PowerBIParser(self.config['pbi_json_path'])
            self.model = self.parser.parse_model()
            results['steps_completed'].append('parse')

            # Step 2: Translate
            print("\n" + "=" * 80)
            print("STEP 2: Translating DAX Measures")
            print("=" * 80)
            self.translator = DAXTranslator(self.model.relationships)
            translation_stats = self.translator.translate_all_measures(self.model)
            results['stats']['translation'] = translation_stats
            print(f"Translation complete: {translation_stats['success']}/{translation_stats['total']} measures")
            results['steps_completed'].append('translate')

            # Step 3: Comments
            if self.config['apply_comments']:
                print("\n" + "=" * 80)
                print("STEP 3: Applying Comments")
                print("=" * 80)
                self.applicator = UnityCatalogApplicator(
                    self.config['target_catalog'],
                    self.config['target_schema'],
                    self.config['dry_run']
                )
                for table in self.model.tables:
                    print(f"\nProcessing table: {table.name}")
                    self.applicator.apply_table_comments(table)
                    self.applicator.apply_column_comments(table)
                results['steps_completed'].append('comments')

            # Step 4: Tags
            if self.config['apply_tags']:
                print("\n" + "=" * 80)
                print("STEP 4: Applying Tags")
                print("=" * 80)
                if not self.applicator:
                    self.applicator = UnityCatalogApplicator(
                        self.config['target_catalog'],
                        self.config['target_schema'],
                        self.config['dry_run']
                    )
                for table in self.model.tables:
                    print(f"\nApplying tags to: {table.name}")
                    self.applicator.apply_table_tags(table)
                    self.applicator.apply_column_tags(table)
                results['steps_completed'].append('tags')

            # Step 5: Views
            if self.config['create_measure_views']:
                print("\n" + "=" * 80)
                print("STEP 5: Creating Measure Views")
                print("=" * 80)
                self.measure_generator = MeasureViewGenerator(
                    self.config['target_catalog'],
                    self.config['target_schema'],
                    self.config['measure_schema'],
                    self.config['dry_run']
                )
                view_statements = self.measure_generator.create_all_measure_views(self.model)
                results['stats']['views_created'] = len(view_statements)
                print(f"\n✓ Created {len(view_statements)} measure views")
                results['steps_completed'].append('views')

            # Step 6: Relationships
            if self.config['create_relationships_table']:
                print("\n" + "=" * 80)
                print("STEP 6: Registering Relationships")
                print("=" * 80)
                self.relationship_registry = RelationshipRegistry(
                    self.config['target_catalog'],
                    self.config['metadata_schema'],
                    self.config['dry_run']
                )
                count = self.relationship_registry.register_relationships(self.model.relationships)
                results['stats']['relationships_registered'] = count
                results['steps_completed'].append('relationships')

            # Step 7: Documentation
            print("\n" + "=" * 80)
            print("STEP 7: Generating Documentation")
            print("=" * 80)
            self.doc_generator = DocumentationGenerator(self.model)
            self.doc_generator.display_summary()
            results['steps_completed'].append('documentation')

            results['success'] = True
            results['stats']['model_summary'] = self.doc_generator.generate_summary_stats()

        except Exception as e:
            results['errors'].append(str(e))
            print(f"\n✗ Pipeline failed: {str(e)}")
            import traceback
            traceback.print_exc()

        return results

    def generate_report(self, results: Dict[str, Any]):
        print("\n" + "=" * 80)
        print("PIPELINE EXECUTION REPORT")
        print("=" * 80)
        print(f"Status: {'SUCCESS' if results['success'] else 'FAILED'}")
        print(f"Steps Completed: {', '.join(results['steps_completed'])}")

        if results.get('stats'):
            print("\nStatistics:")
            for key, value in results['stats'].items():
                if isinstance(value, dict):
                    print(f"  {key}:")
                    for k, v in value.items():
                        print(f"    {k}: {v}")
                else:
                    print(f"  {key}: {value}")

        if results.get('errors'):
            print("\nErrors:")
            for error in results['errors']:
                print(f"  - {error}")

        print("=" * 80)


# COMMAND ----------
# COMMAND 14: Pipeline execution
orchestrator = SemanticPushDownOrchestrator(config)
results = orchestrator.run()
orchestrator.generate_report(results)

# COMMAND ----------
# COMMAND 15: Manual translation queue export
if orchestrator.model:
    manual_measures = []
    for table in orchestrator.model.tables:
        for measure in table.measures:
            if measure.translation_status == 'manual_required':
                manual_measures.append({
                    'Table': table.name,
                    'Measure': measure.name,
                    'DAX Expression': measure.expression,
                    'Description': measure.description,
                    'Translation Notes': measure.translation_notes,
                    'Display Folder': measure.display_folder
                })

    if manual_measures:
        import hashlib
        manual_df = pd.DataFrame(manual_measures)
        manual_df["execution_ts"] = datetime.now().isoformat()
        manual_df["measure_id"] = manual_df.apply(
            lambda r: hashlib.md5(f"{r['Table']}|{r['Measure']}".encode("utf-8")).hexdigest(),
            axis=1
        )
        cols = ["measure_id", "execution_ts"] + [c for c in manual_df.columns if c not in ("measure_id", "execution_ts")]
        manual_df = manual_df[cols]

        print(f"\n⚠ {len(manual_measures)} measures require manual translation\n")
        display(manual_df)

        if not config['dry_run']:
            try:
                spark.createDataFrame(manual_df).write.mode('overwrite').saveAsTable(
                    f"{config['target_catalog']}.{config['metadata_schema']}.manual_translation_queue"
                )
                print(f"\n✓ Saved to: {config['target_catalog']}.{config['metadata_schema']}.manual_translation_queue")
            except Exception as e:
                print(f"✗ Could not save table: {str(e)}")
                print("  Saving to temp view instead...")
                spark.createDataFrame(manual_df).createOrReplaceTempView("manual_translation_queue_tmp")
                print("✓ Created temp view: manual_translation_queue_tmp")
    else:
        print("\n✓ All measures were successfully translated!")
