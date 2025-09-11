# scripts/render.py
from jinja2 import Environment, FileSystemLoader, select_autoescape
import os

def render_dashboard(docspath, ctx):
    os.makedirs(docspath, exist_ok=True)
    env = Environment(
        loader=FileSystemLoader("scripts"),
        autoescape=select_autoescape(["html", "xml"]),
    )
    tpl = env.get_template("template.html")
    html = tpl.render(**ctx)

    with open(os.path.join(docspath, ".nojekyll"), "w", encoding="utf-8") as f:
        f.write("")
    with open(os.path.join(docspath, "index.html"), "w", encoding="utf-8") as f:
        f.write(html)
