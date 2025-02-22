# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

from datetime import datetime
# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import sys
import pathlib

here = pathlib.Path(__file__).parent
sys.path.insert(0, str(here.parent))

import omnipath_server  # noqa: E402

# -- Project information -----------------------------------------------------

project = 'omnipath_server'
version = omnipath_server.__version__
author = ', '.join(omnipath_server.__author__)
years = '-'.join(sorted({'2022', f'{datetime.now():%Y}'}))
copyright = f'{years}, Saez Lab'
repository_url = 'https://github.com/saezlab/omnipath-server'

# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named "sphinx.ext.*") or your custom
# ones.
extensions = [
    'myst_parser',
    'sphinx.ext.autodoc',
    'sphinx.ext.intersphinx',
    'sphinx.ext.autosummary',
    'sphinx.ext.napoleon',
    'sphinx.ext.todo',  # not for output but to remove warnings
    'sphinx.ext.githubpages',
    'sphinx.ext.viewcode',
    'sphinx.ext.ifconfig',
    'sphinxcontrib.bibtex',
    'sphinx_autodoc_typehints',
    'sphinx.ext.mathjax',
    'sphinx_copybutton',
    'sphinx_last_updated_by_git',
    'sphinxcontrib.fulltoc',
    'sphinx_remove_toctrees',
    'nbsphinx',
    'IPython.sphinxext.ipython_console_highlighting',
]

autosummary_generate = True
autodoc_member_order = 'groupwise'
default_role = 'literal'
napoleon_google_docstring = False
napoleon_numpy_docstring = True
napoleon_include_init_with_doc = False
napoleon_use_rtype = True  # having a separate entry generally helps readability
napoleon_use_param = True

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# The language for content autogenerated by Sphinx. Refer to documentation
# for a list of supported languages.
#
# This is also used if you do content translation via gettext catalogs.
# Usually you set "language" from the command line for these cases.
language = 'en'

# The suffix(es) of source filenames.
# You can specify multiple suffix as a list of string:
#
source_suffix = ['.rst', '.md']

# The master toctree document.
master_doc = 'contents'

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store', '**.ipynb_checkpoints']


# -- Autodoc configuration ---------------------------------------------------

autodoc_mock_imports = []

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = 'manni'

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.

html_theme = 'pydata_sphinx_theme'
html_theme_options = {
    'navigation_depth': 2,
    'collapse_navigation': True,
}
html_context = {
    'display_github': True,  # Integrate GitHub
    'github_user': 'saezlab',  # Username
    'github_repo': project,  # Repo name
    'github_version': 'main',  # Version
    'conf_py_path': '/docs/',  # Path in the checkout to the docs root
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']

nitpick_ignore = [
    # If building the documentation fails because
    # of a missing link that is outside your control,
    # you can add an exception to this list.
    #     ("py:class", "igraph.Graph"),
]
