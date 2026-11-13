project = "Takler Test Suite"
language = "zh_CN"
extensions = ["myst_parser"]
nitpicky = True

# Keep the same heading fragments in the Markdown sources and the HTML build.
myst_heading_anchors = 3
myst_heading_anchors_html_ids = True

html_theme = "pydata_sphinx_theme"
html_title = "Takler Test Suite 文档"
html_theme_options = {
    "github_url": "https://github.com/cemc-oper/takler-test-suite",
    "navigation_with_keys": False,
    "show_toc_level": 2,
}
