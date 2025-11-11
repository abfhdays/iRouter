"""SQL parsing and optimization using SQLGlot."""
from typing import List, Optional, Any
import sqlglot
from sqlglot import exp
from sqlglot.optimizer import optimize
from sqlglot.optimizer import (
    qualify,
    qualify_columns,
    simplify,
    pushdown_predicates,
    normalize,
    annotate_types,
)

from irouter.core.types import (
    Predicate, 
    PredicateOperator, 
    PredicateExtractionResult
)


class SQLParser:
    """Parses and optimizes SQL queries using SQLGlot."""
    
    def __init__(self, dialect: str = "spark"):
        """
        Initialize parser.
        
        Args:
            dialect: SQL dialect (spark, postgres, snowflake, etc.)
        """
        self.dialect = dialect
    
    def parse(self, sql: str) -> exp.Expression:
        """
        Parse SQL string into AST.
        
        Args:
            sql: SQL query string
            
        Returns:
            SQLGlot expression (AST)
        """
        try:
            return sqlglot.parse_one(sql, dialect=self.dialect)
        except Exception as e:
            raise ValueError(f"Failed to parse SQL: {e}")
    
    def optimize(
        self, 
        ast: exp.Expression, 
        schema: Optional[dict] = None,
        rules: Optional[List] = None
    ) -> exp.Expression:
        """
        Apply optimization rules to AST.
        
        Args:
            ast: Parsed SQL expression
            schema: Optional schema information
            rules: Specific rules to apply (default: all important ones)
            
        Returns:
            Optimized AST
        """
        if rules is None:
            # Default rules for partition pruning
            rules = [
                qualify.qualify,
                pushdown_predicates.pushdown_predicates,
                simplify.simplify,
                normalize.normalize,
            ]
            
            # Add column qualification with schema if provided
            if schema is not None:
                rules.insert(1, qualify_columns.qualify_columns)
                rules.append(annotate_types.annotate_types)
        
        try:
            return optimize(
                ast, 
                schema=schema, 
                dialect=self.dialect,
                rules=rules,
                validate_qualify_columns=False
            )
        except Exception as e:
            print(f"Warning: Optimization failed: {e}")
            return ast
    
    def extract_tables(self, ast: exp.Expression) -> List[str]:
        """
        Extract all table names from query.
        
        Args:
            ast: Parsed SQL expression
            
        Returns:
            List of table names
        """
        tables = []
        for table in ast.find_all(exp.Table):
            if isinstance(table.this, exp.Identifier):
                tables.append(table.this.name)
            else:
                tables.append(str(table.this))
        return tables
    
    def extract_where_clause(self, ast: exp.Expression) -> Optional[exp.Where]:
        """
        Extract WHERE clause from query.
        
        Args:
            ast: Parsed SQL expression
            
        Returns:
            WHERE clause or None
        """
        return ast.find(exp.Where)
    
    def extract_predicates(
        self, 
        ast: exp.Expression, 
        table_name: Optional[str] = None
    ) -> PredicateExtractionResult:
        """
        Extract predicates from WHERE clause.
        
        This is the KEY method for partition pruning.
        
        Args:
            ast: Parsed and optimized SQL expression
            table_name: Filter predicates for this table only
            
        Returns:
            PredicateExtractionResult with extracted predicates
        """
        where = self.extract_where_clause(ast)
        
        if not where:
            return PredicateExtractionResult(
                predicates=[],
                table_name=table_name or "",
                is_complex=False
            )
        
        predicates = []
        is_complex = False
        
        condition = where.this
        
        # Check for complex conditions (OR, NOT)
        if self._has_or_not(condition):
            is_complex = True
        
        # Extract simple predicates (AND chains)
        predicates = self._extract_simple_predicates(condition, table_name)
        
        return PredicateExtractionResult(
            predicates=predicates,
            table_name=table_name or "",
            is_complex=is_complex
        )
    
    def _has_or_not(self, expression: exp.Expression) -> bool:
        """Check if expression contains OR or NOT."""
        for node in expression.walk():
            if isinstance(node, (exp.Or, exp.Not)):
                return True
        return False
    
    def _extract_simple_predicates(
        self, 
        expression: exp.Expression,
        table_filter: Optional[str] = None
    ) -> List[Predicate]:
        """
        Extract simple predicates from expression.
        
        Handles: column = value, column > value, etc.
        Skips: OR conditions, NOT conditions
        
        Args:
            expression: SQLGlot expression to analyze
            table_filter: Only extract predicates for this table
            
        Returns:
            List of Predicate objects
        """
        predicates = []
        
        # Handle AND chains
        if isinstance(expression, exp.And):
            predicates.extend(self._extract_simple_predicates(expression.left, table_filter))
            predicates.extend(self._extract_simple_predicates(expression.right, table_filter))
            return predicates
        
        # Handle comparison operators
        comparison_types = {
            exp.EQ: PredicateOperator.EQ,
            exp.NEQ: PredicateOperator.NEQ,
            exp.GT: PredicateOperator.GT,
            exp.GTE: PredicateOperator.GTE,
            exp.LT: PredicateOperator.LT,
            exp.LTE: PredicateOperator.LTE,
            exp.In: PredicateOperator.IN,
        }
        
        for exp_type, pred_op in comparison_types.items():
            if isinstance(expression, exp_type):
                predicate = self._build_predicate(expression, pred_op, table_filter)
                if predicate:
                    predicates.append(predicate)
                break
        
        return predicates
    
    def _build_predicate(
        self,
        expression: exp.Expression,
        operator: PredicateOperator,
        table_filter: Optional[str] = None
    ) -> Optional[Predicate]:
        """
        Build a Predicate from a comparison expression.
        
        Args:
            expression: Comparison expression
            operator: Predicate operator
            table_filter: Only create predicate if column is from this table
            
        Returns:
            Predicate object or None
        """
        left = expression.left if hasattr(expression, 'left') else expression.this
        right = expression.right if hasattr(expression, 'right') else expression.expression
        
        # Left side should be a column
        if not isinstance(left, exp.Column):
            return None
        
        # Extract column info
        column_name = left.name
        table_name = None
        
        # Get table qualifier if present
        if left.table:
            table_name = left.table
        
        # Filter by table if specified
        if table_filter and table_name and table_name != table_filter:
            return None
        
        # Extract value
        value = self._extract_value(right)
        
        # Get SQL type if available
        sql_type = None
        if hasattr(left, 'type') and left.type:
            sql_type = str(left.type)
        
        return Predicate(
            column=column_name,
            operator=operator,
            value=value,
            sql_type=sql_type
        )
    
    def _extract_value(self, expression: exp.Expression) -> Any:
        """
        Extract literal value from expression.
        
        Args:
            expression: SQLGlot expression
            
        Returns:
            Python value
        """
        if isinstance(expression, exp.Literal):
            return expression.this
        
        elif isinstance(expression, exp.Tuple):
            return [self._extract_value(e) for e in expression.expressions]
        
        elif isinstance(expression, exp.Null):
            return None
        
        elif isinstance(expression, exp.Boolean):
            return expression.this
        
        else:
            return str(expression)
    
    def to_sql(self, ast: exp.Expression, pretty: bool = True) -> str:
        """
        Convert AST back to SQL string.
        
        Args:
            ast: SQLGlot expression
            pretty: Pretty-print with formatting
            
        Returns:
            SQL string
        """
        return ast.sql(dialect=self.dialect, pretty=pretty)