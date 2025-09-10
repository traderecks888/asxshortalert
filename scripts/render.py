from jinja2 import Environment, FileSystemLoader
import os

def render_dashboard(docspath, ctx):
    os.makedirs(docspath, exist_ok=True)
    env = Environment(loader=FileSystemLoader("scripts"))
    tpl = env.get_template("template.html")
    html = tpl.render(**ctx)
    with open(os.path.join(docspath, "index.html"), "w", encoding="utf-8") as f:
        f.write(html)
