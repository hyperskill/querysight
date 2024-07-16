from openai import OpenAI
from typing import List, Dict


class AISuggester:
    def __init__(self, api_key: str):
        self.client = OpenAI(api_key=api_key)

    def generate_suggestions(self, query_analysis: Dict, dbt_structure: Dict) -> List[str]:
        prompt = f"""
        Given the following ClickHouse query analysis:
        {query_analysis}

        And the current dbt project structure:
        {dbt_structure}

        Suggest improvements for the dbt project to optimize for the most common queries and data patterns.
        """

        response = self.client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}]
        )

        return response.choices[0].message.content.split('\n')
