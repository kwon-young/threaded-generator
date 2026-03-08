import os
import sys
import tomllib

# Point Sphinx to the source code
sys.path.insert(0, os.path.abspath('../src'))

with open("../pyproject.toml", "rb") as f:
    toml = tomllib.load(f)
    pyproject = toml["project"]

project = pyproject["name"]
author = pyproject["authors"][0]["name"]
copyright = f"2026, {author}"
release = pyproject["version"]
version = ".".join(release.split(".")[:2])

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.viewcode',
    'myst_parser',
]

templates_path = ['_templates']
exclude_patterns: list[str] = []

html_theme = 'alabaster'

# Render type hints in description
autodoc_typehints = "description"

# Don't show the full module path in the signature
add_module_names = False

# Put each parameter on a separate line if the signature is long
maximum_signature_line_length = 80
