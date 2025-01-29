"""SQL parsing utilities for QuerySight."""
import sqlparse
from sqlparse.sql import Token, TokenList, Identifier, Function, Parenthesis
from sqlparse.tokens import Keyword, Name, DML, Punctuation
from typing import Set, List, Optional
import re
import logging

logger = logging.getLogger(__name__)

class SQLTableExtractor:
    """Extract table references from SQL queries with support for complex cases."""
    
    def __init__(self):
        self.table_refs = set()
        self.cte_names = set()  # Track CTE names to avoid treating them as table refs
        
    def _clean_identifier(self, identifier: str) -> str:
        """Clean and normalize table identifiers."""
        # Remove quotes and backticks
        clean = re.sub(r'[`"\']+', '', identifier)
        # Remove alias if present (handling both 'AS alias' and plain 'alias')
        clean = re.split(r'\s+(?=AS\s+|\w+)', clean)[0]
        return clean.strip()
    
    def _extract_from_token(self, token_value: str) -> Set[str]:
        """Extract table reference variations from a token value."""
        if not token_value or token_value.lower() in self.cte_names:
            return set()
            
        # Clean the identifier first
        token_value = self._clean_identifier(token_value)
        parts = token_value.split('.')
        variations = set()
        
        if len(parts) == 1:
            variations.add(parts[0])
        elif len(parts) == 2:
            schema, table = parts
            variations.add(f"{schema}.{table}")
            variations.add(table)
        elif len(parts) == 3:
            database, schema, table = parts
            variations.add(f"{database}.{schema}.{table}")
            variations.add(f"{schema}.{table}")
            variations.add(table)
            
        return {v.lower() for v in variations if v}
    
    def _process_identifier(self, identifier: Identifier) -> None:
        """Process an SQL identifier token to extract table references."""
        # Skip if this is a CTE name
        identifier_str = str(identifier)
        if self._clean_identifier(identifier_str).lower() in self.cte_names:
            return
            
        # Get the real name part
        name_parts = []
        for token in identifier.tokens:
            if token.is_whitespace:
                # Stop at first whitespace - anything after is likely an alias
                break
            if isinstance(token, (Identifier, Function)):
                name_parts.append(token.value)
            elif token.ttype in (Name, Name.Placeholder) or token.value == '.':
                name_parts.append(token.value)
        
        if name_parts:
            # Join parts and clean
            table_name = ''.join(name_parts)
            table_variations = self._extract_from_token(table_name)
            self.table_refs.update(table_variations)
    
    def _process_function(self, func: Function) -> None:
        """Process function calls, specifically for dbt ref() and source() functions."""
        if not func.tokens:
            return
            
        func_name = func.tokens[0].value.lower()
        if func_name in ('ref', 'source'):
            # Extract arguments from ref('model_name') or source('source_name', 'table_name')
            args = []
            for token in func.tokens:
                if token.ttype == Name:
                    args.append(self._clean_identifier(token.value))
                    
            if func_name == 'ref' and len(args) >= 1:
                # ref('model_name')
                self.table_refs.add(args[0])
            elif func_name == 'source' and len(args) >= 2:
                # source('source_name', 'table_name')
                self.table_refs.add(f"{args[0]}.{args[1]}")
    
    def _extract_cte_names(self, token_list: TokenList) -> None:
        """Extract CTE names to avoid treating them as table references."""
        in_cte = False
        for token in token_list.tokens:
            if token.ttype is Keyword.CTE and token.value.upper() == 'WITH':
                in_cte = True
                continue
                
            if in_cte and isinstance(token, Identifier):
                # Add CTE name to ignore list
                cte_name = self._clean_identifier(str(token))
                self.cte_names.add(cte_name.lower())
                
            if isinstance(token, Parenthesis):
                # Process the CTE definition for table references
                self._process_token_list(token)
                
            # End CTE section when we hit a SELECT
            if token.ttype is DML and token.value.upper() == 'SELECT':
                in_cte = False
    
    def _process_token_list(self, token_list: TokenList) -> None:
        """Recursively process a token list to find table references."""
        # First pass: collect CTE names
        self._extract_cte_names(token_list)
        
        is_from = False
        is_join = False
        
        for token in token_list.tokens:
            # Handle FROM and JOIN keywords
            if token.ttype is Keyword:
                upper_val = token.value.upper()
                if upper_val == 'FROM':
                    is_from = True
                    is_join = False
                elif 'JOIN' in upper_val:
                    is_join = True
                    is_from = False
                continue
                
            # Process subqueries in CTEs
            if isinstance(token, Parenthesis):
                self._process_token_list(token)
                continue
                
            # Skip if not after FROM/JOIN
            if not (is_from or is_join):
                continue
                
            # Process the token
            if isinstance(token, TokenList):
                if isinstance(token, Identifier):
                    self._process_identifier(token)
                else:
                    self._process_token_list(token)
            elif isinstance(token, Function):
                self._process_function(token)
            elif token.ttype is None and not token.is_whitespace:
                # This might be a table reference
                table_variations = self._extract_from_token(token.value)
                self.table_refs.update(table_variations)
            
            # Reset flags after non-whitespace tokens
            if not token.is_whitespace:
                is_from = False
                is_join = False

    def extract_tables(self, sql: str) -> Set[str]:
        """
        Extract table references from a SQL query.
        
        Args:
            sql: SQL query string
            
        Returns:
            Set of table names referenced in the query
        """
        try:
            # Reset state
            self.table_refs.clear()
            self.cte_names.clear()
            
            # Parse SQL
            statements = sqlparse.parse(sql)
            
            # Process each statement
            for statement in statements:
                if statement.get_type() in ('SELECT', 'INSERT', 'UPDATE', 'DELETE'):
                    self._process_token_list(statement)
            
            return self.table_refs
            
        except Exception as e:
            logger.error(f"Error parsing SQL: {str(e)}")
            logger.error(f"Problematic SQL: {sql}")
            return set()  # Return empty set on error

def extract_tables_from_query(sql: str) -> Set[str]:
    """Convenience function to extract table references from a SQL query."""
    extractor = SQLTableExtractor()
    return extractor.extract_tables(sql)
