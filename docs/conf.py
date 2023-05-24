# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "aiodiskqueue"
copyright = "2023, Erik Kalkoken"
author = "Erik Kalkoken"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = ["sphinx.ext.autodoc", "sphinx.ext.napoleon"]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# -- Options for autodoc -----------------------------------------------------
# autoclass_content = "both"
autodoc_default_options = {
    "members": True,
    "member-order": "alphabetical",
    "special-members": "__init__",
    "undoc-members": True,
    "exclude-members": "__weakref__",
}

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "alabaster"
html_theme_options = {
    "description": "Persistent queues for Python AsyncIO",
    "fixed_sidebar": True,
    "badge_branch": "master",
    "github_button": True,
    "github_user": "ErikKalkoken",
    "github_repo": "aiodiskqueue",
    "show_powered_by": False,
    "sidebar_collapse": False,
    "extra_nav_links": {
        "Report Issues": "https://github.com/ErikKalkoken/aiodiskqueue/issues",
    },
}

html_sidebars = {
    "**": [
        "about.html",
        "navigation.html",
        "relations.html",
        "searchbox.html",
    ],
}

html_static_path = ["_static"]
html_css_files = ["custom.css"]
