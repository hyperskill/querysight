#!/usr/bin/env python3

"""
Test script for SQL parser to identify limitations with complex queries.
This script tests the SQLTableExtractor against several complex query patterns
and reports success/failure for each case.
"""

import logging
import sys
from typing import Dict, List, Set, Tuple
from utils.sql_parser import SQLTableExtractor, extract_tables_from_query

# Configure logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('sql_parser_test')

# Test cases
TEST_CASES = [
    {
        "name": "Simple query",
        "sql": """
        SELECT * FROM my_schema.my_table
        """,
        "expected_tables": {"my_schema.my_table"}
    },
    {
        "name": "Join query",
        "sql": """
        SELECT a.*, b.* 
        FROM schema_a.table_a a
        JOIN schema_b.table_b b ON a.id = b.id
        """,
        "expected_tables": {"schema_a.table_a", "schema_b.table_b"}
    },
    {
        "name": "Subquery",
        "sql": """
        SELECT * FROM (
            SELECT * FROM inner_schema.inner_table
        ) t
        """,
        "expected_tables": {"inner_schema.inner_table"}
    },
    {
        "name": "CTE query",
        "sql": """
        WITH cte_name AS (
            SELECT * FROM cte_schema.cte_table
        )
        SELECT * FROM cte_name
        JOIN other_schema.other_table ON cte_name.id = other_table.id
        """,
        "expected_tables": {"cte_schema.cte_table", "other_schema.other_table"}
    },
    {
        "name": "Complex CTE with numeric expression",
        "sql": """
        WITH 28 * 6 AS days_ago 
        SELECT user_id, 'Google One Tap', dt, date, platform
        FROM hyperskill.content
        WHERE date > today() - days_ago - 1
        AND action = 'logged_in_using_google_one_tap'
        AND date = registration_date
        """,
        "expected_tables": {"hyperskill.content"}
    },
    {
        "name": "Complex virtual table query",
        "sql": """
        SELECT toMonday(toDateTime(`date`)) AS `date_5fc732`,
               count(DISTINCT `user_id`) AS `Total_96b014`,
               uniqExactIf(user_id, platform = 'web') AS `Web_c6e190`,
               uniqExactIf(user_id, platform = 'android mobile browser') AS `Android Mobile Browser_4e79f0`
        FROM
          (WITH 28 * 6 AS days_ago SELECT user_id,
                                          'Google One Tap',
                                          dt, date, platform
           FROM hyperskill.content
           WHERE date > today() - days_ago - 1
             AND action = 'logged_in_using_google_one_tap'
             AND date = registration_date) AS `virtual_table`
        WHERE `date` >= toDate('2024-02-20')
          AND `date` < toDate('2025-02-20')
        GROUP BY toMonday(toDateTime(`date`))
        ORDER BY `Total_96b014` DESC
        LIMIT 1000
         FORMAT Native
        """,
        "expected_tables": {"hyperskill.content"}
    },
    {
        "name": "Query with complex functions",
        "sql": """
        SELECT `skill_type` AS `skill_type_c49ace`
        FROM
          (SELECT s.name AS skill,
                  s.type AS skill_type,
                  s.domain AS skill_domain,
                  v.job_title AS job_title,
                  c.name AS cluster_name,
                  skills_vacancies.requirements as requirements,
                  v.link AS link,
                  v.level AS level,
                  v.description AS description,
                  v.location AS location,
                  v.company AS company,
                  v.source_id AS source_id,
                  COUNT(DISTINCT v.source_id) OVER() AS unique_vacancy_count,
                  COALESCE(toDateTime(v.publication_date), toDateTime('2000-01-01 00:00:00')) AS publication_date
           FROM hyperskill_positions_db.skills_vacancies
           LEFT JOIN hyperskill_positions_db.skills s ON skills_vacancies.skill_id = s.id
           LEFT JOIN hyperskill_positions_db.vacancies v ON skills_vacancies.vacancy_id = v.id
           LEFT JOIN hyperskill_positions_db.vacancies_clusters vc ON v.id = vc.vacancy_id
           LEFT JOIN hyperskill_positions_db.clusters c ON vc.cluster_id = c.id
           ORDER BY source_id,
                    requirements,
                    skill_domain) AS `virtual_table`
        GROUP BY `skill_type`
        ORDER BY `skill_type` ASC
        LIMIT 1000
         FORMAT Native
        """,
        "expected_tables": {
            "hyperskill_positions_db.skills_vacancies", 
            "hyperskill_positions_db.skills", 
            "hyperskill_positions_db.vacancies", 
            "hyperskill_positions_db.vacancies_clusters", 
            "hyperskill_positions_db.clusters"
        }
    },
    {
        "name": "Query with dictGet functions",
        "sql": """
        SELECT concat('<a target="_blank" rel="noopener noreferrer" href="https://hyperskill.org/admin/r/14/',
               toString(content_id), '/',
               '">',
               if((dictGetString('hyperskill.alt_steps_step_dict', 'title', toUInt64(content_id)) AS step_title)= '',
               if((dictGetString('hyperskill.alt_topics_topic_dict', 'title',
               toUInt64(dictGetInt32('hyperskill.alt_steps_step_dict', 'topic_id', toUInt64(content_id)))) AS topic_title) = '',
               concat(dictGetString('hyperskill.alt_projects_project_dict', 'title',
               toUInt64(dictGetInt32('hyperskill_reports.stages_stage_by_step_id', 'project_id', toUInt64(content_id)))), ' -> ',
               dictGetString('hyperskill_reports.stages_stage_by_step_id', 'title', toUInt64(content_id))),
               topic_title),
               step_title), '</a>') AS link
        FROM hyperskill_mariadb.comments_comment
        """,
        "expected_tables": {
            "hyperskill.alt_steps_step_dict",
            "hyperskill.alt_topics_topic_dict",
            "hyperskill.alt_projects_project_dict",
            "hyperskill_reports.stages_stage_by_step_id",
            "hyperskill_mariadb.comments_comment"
        }
    },
    {
        "name": "Query with arrayJoin",
        "sql": """
        SELECT `user_type` AS `user_type_d7b516`
        FROM
          (SELECT arrayJoin(['Personal', 'Team Member', 'Other', 'Freemium', 'Premium']) AS user_type) 
          AS `virtual_table`
        GROUP BY `user_type`
        ORDER BY `user_type` ASC
        LIMIT 1000
        FORMAT Native
        """,
        "expected_tables": set()  # No actual tables referenced
    }
]

def test_sql_parser() -> Dict[str, List[Tuple[str, bool, Set[str], Set[str]]]]:
    """
    Test the SQL parser against various test cases.
    
    Returns:
        Dict with 'passed' and 'failed' lists of test results
    """
    results = {
        "passed": [],
        "failed": []
    }
    
    for test_case in TEST_CASES:
        name = test_case["name"]
        sql = test_case["sql"]
        expected_tables = test_case["expected_tables"]
        
        # Test the parser
        try:
            actual_tables = extract_tables_from_query(sql)
            
            if actual_tables == expected_tables:
                logger.info(f"✅ Test PASSED: {name}")
                results["passed"].append((name, True, expected_tables, actual_tables))
            else:
                logger.error(f"❌ Test FAILED: {name}")
                logger.error(f"  Expected: {expected_tables}")
                logger.error(f"  Actual:   {actual_tables}")
                logger.error(f"  Missing:  {expected_tables - actual_tables}")
                logger.error(f"  Extra:    {actual_tables - expected_tables}")
                results["failed"].append((name, False, expected_tables, actual_tables))
                
        except Exception as e:
            logger.error(f"❌ Test ERROR: {name} - {str(e)}")
            results["failed"].append((name, False, expected_tables, set()))
    
    return results

def print_summary(results):
    """Print a summary of test results"""
    total = len(results["passed"]) + len(results["failed"])
    passed = len(results["passed"])
    
    print("\n" + "="*80)
    print(f"SQL PARSER TEST SUMMARY: {passed}/{total} tests passed ({passed/total:.1%})")
    print("="*80)
    
    if results["failed"]:
        print("\nFailed tests:")
        for name, _, expected, actual in results["failed"]:
            print(f"  - {name}")
            print(f"    Expected: {expected}")
            print(f"    Actual:   {actual}")
    
    print("\n")

if __name__ == "__main__":
    results = test_sql_parser()
    print_summary(results)
    
    # Exit with error code if any tests failed
    sys.exit(1 if results["failed"] else 0)
