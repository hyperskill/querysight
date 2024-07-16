import os
import yaml
from typing import List, Dict


class DBTProjectAnalyzer:
    def __init__(self, project_dir: str):
        self.project_dir = project_dir

    def analyze_project(self) -> Dict:
        project_structure = {
            'models': [],
            'sources': [],
            'macros': []
        }

        for root, dirs, files in os.walk(self.project_dir):
            for file in files:
                if file.endswith('.sql'):
                    with open(os.path.join(root, file), 'r') as f:
                        content = f.read()
                        # Parse SQL file and extract relevant information
                        # Add to project_structure
                elif file.endswith('.yml'):
                    with open(os.path.join(root, file), 'r') as f:
                        yaml_content = yaml.safe_load(f)
                        # Parse YAML file and extract relevant information
                        # Add to project_structure

        return project_structure