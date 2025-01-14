"""SQL parsing utilities for QuerySight."""
import sqlparse
from sqlparse.sql import Token, TokenList, Identifier, Function
from sqlparse.tokens import Keyword, Name, DML, Punctuation
from typing import Set, List, Optional
import re
import logging

logger = logging.getLogger(__name__)

class SQLTableExtractor:
    """Extract table references from SQL queries with support for complex cases."""
    
    def __init__(self):
        self.table_refs = set()
        self.current_cte_names = set()  # Track CTE names to avoid treating them as table refs
        
    def _clean_identifier(self, identifier: str) -> str:
        """Clean and normalize table identifiers."""
        # Remove quotes and backticks
        clean = re.sub(r'[`"\']+', '', identifier)
        # Take last part of qualified name (e.g., schema.table -> table)
        parts = clean.split('.')
        return parts[-1].strip().lower()
    
    def _extract_from_token(self, token: Token) -> List[str]:
        """Extract table name variations from a token, handling quoted and qualified names."""
        if not token:
            return []
            
        # Handle quoted identifiers
        value = token.value.strip('`"\' ')
        # Split on dots for qualified names
        parts = [p.strip('`"\' ') for p in value.split('.')]
        
        if not parts:
            return []
        
        # Generate all possible variations
        variations = []
        # Just table name
        variations.append(parts[-1].lower())
        
        if len(parts) >= 2:
            # schema.table
            variations.append(f"{parts[-2]}.{parts[-1]}".lower())
        
        if len(parts) >= 3:
            # database.schema.table
            variations.append(f"{parts[-3]}.{parts[-2]}.{parts[-1]}".lower())
            
        return variations
    
    def _process_identifier(self, identifier: Identifier) -> None:
        """Process an SQL identifier token to extract table references."""
        # Skip if this is a CTE name
        if identifier.value.lower() in self.current_cte_names:
            return
            
        # Get the real name part
        name_token = None
        for token in identifier.tokens:
            if token.ttype in (Name, Name.Placeholder):
                name_token = token
                break
        
        if name_token:
            table_variations = self._extract_from_token(name_token)
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
    
    def _extract_cte_names(self, statement: TokenList) -> None:
        """Extract CTE (Common Table Expression) names to avoid treating them as table refs."""
        with_seen = False
        for token in statement.tokens:
            # Look for WITH keyword
            if token.ttype is Keyword and token.value.upper() == 'WITH':
                with_seen = True
                continue
                
            if with_seen and isinstance(token, Identifier):
                # Add CTE name to our set
                cte_name = self._clean_identifier(token.value)
                if cte_name:
                    self.current_cte_names.add(cte_name)
                    
            # Stop looking after we find the first non-CTE token
            if with_seen and token.ttype is Keyword and token.value.upper() != 'WITH':
                break
    
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
                is_from = upper_val == 'FROM'
                is_join = 'JOIN' in upper_val
                continue
                
            # Skip if not after FROM/JOIN
            if not (is_from or is_join):
                continue
                
            # Reset flags
            is_from = False
            is_join = False
            
            if isinstance(token, TokenList):
                self._process_token_list(token)
            elif isinstance(token, Identifier):
                self._process_identifier(token)
            elif isinstance(token, Function):
                self._process_function(token)
    
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
            self.current_cte_names.clear()
            
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
