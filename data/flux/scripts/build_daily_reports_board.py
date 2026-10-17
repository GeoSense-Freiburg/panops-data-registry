#!/usr/bin/env python3
"""Build the "Daily Reports" board for the fluxVSmortality GitHub Pages site.

Reads every daily_reports/*.md, renders each to a standalone dark-theme HTML page with pandoc,
copies the figures it references into the site folder, writes an index board, and makes sure the
hub page (fluxVSmortality/index.html) has a card linking to it. Re-run after adding or editing a
report, then publish with:  bash scripts/push_fluxVSmortality.sh "message"

Output (all inside the site folder): fluxVSmortality/daily_reports/{index.html,<report>.html,img/*}
Pages carry noindex/nofollow (still readable by anyone who has the link).
"""
import datetime, hashlib, html, os, re, shutil, subprocess, sys

ROOT = "/mnt/gsdata/projects/panops/panops-data-registry/data/flux"
SRC = os.path.join(ROOT, "daily_reports")
SITE = os.path.join(ROOT, "fluxVSmortality")
OUT = os.path.join(SITE, "daily_reports")
IMG = os.path.join(OUT, "img")
PANDOC = "/home/nk1125/miniconda3/envs/clean_r_env/bin/pandoc"
# Reports left off the public site. Empty = publish every report in daily_reports/. On 2026-09-25 the
# three September reports (they name coauthors and discuss meeting feedback / how to answer reviewers)
# were first held back, then published at the user's explicit request ("publish all nine"). To hold a
# report back again add its file name here; the output folder is cleared on each run, so it cannot linger.
EXCLUDE = set()

CSS = """
*{box-sizing:border-box;margin:0;padding:0}
body{background:#0D0D0D;color:#E0E0E0;font-family:Arial,Helvetica,sans-serif;line-height:1.6;padding:28px 18px 60px}
a{color:#22D4EB;text-decoration:none} a:hover{text-decoration:underline}
.wrap{max-width:900px;margin:0 auto}
.nav{font-size:.85em;color:#6B7280;margin-bottom:22px} .nav a{margin-right:14px}
h1{font-size:1.65em;color:#fff;line-height:1.25;margin-bottom:6px;letter-spacing:-.01em}
.article h1{display:none}
.article h2{font-size:1.3em;color:#22D4EB;margin:34px 0 12px;padding-bottom:6px;border-bottom:1px solid #22D4EB55}
.article h3{font-size:1.1em;color:#E5E7EB;margin:24px 0 8px}
.article h4{font-size:1em;color:#9CA3AF;margin:18px 0 6px}
.article p{margin:10px 0} .article ul,.article ol{margin:8px 0 8px 24px} .article li{margin:4px 0}
.article hr{border:0;border-top:1px solid #2A2A3A;margin:26px 0}
.article blockquote{border-left:3px solid #22D4EB;padding:2px 14px;color:#9CA3AF;margin:12px 0}
.article code{background:#1F2937;border-radius:4px;padding:1px 5px;font-size:.88em;color:#A7F3D0;font-family:Consolas,Menlo,monospace}
.article pre{background:#111827;border:1px solid #2A2A3A;border-radius:8px;padding:12px 14px;overflow-x:auto;margin:12px 0}
.article pre code{background:none;padding:0;color:#E5E7EB}
.tw{overflow-x:auto;margin:14px 0}
.article table{border-collapse:collapse;font-size:.88em;min-width:60%}
.article th{background:#1F2937;color:#22D4EB;text-align:left}
.article th,.article td{border:1px solid #2A2A3A;padding:6px 10px;vertical-align:top}
.article tr:nth-child(even) td{background:#111827}
.article img{max-width:100%;height:auto;background:#fff;border-radius:6px;margin:12px 0 4px;display:block}
.article em{color:#9CA3AF}
.badge{display:inline-block;font-size:.75em;background:#1F2937;border:1px solid #374151;border-radius:4px;padding:2px 8px;color:#9CA3AF;margin-right:6px}
.meta{margin:6px 0 22px}
.footer{margin-top:50px;font-size:.75em;color:#4B5563;text-align:center}
"""

BOARD_CSS = """
.month{font-size:1.05em;color:#22D4EB;margin:34px 0 12px;padding-bottom:6px;border-bottom:1px solid #22D4EB55}
.cards{display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:16px}
.card{display:flex;flex-direction:column;gap:8px;background:#111827;border:1px solid #2A2A3A;border-radius:12px;padding:20px 20px;color:#E0E0E0;transition:border-color .2s,transform .15s}
.card:hover{border-color:#22D4EB;transform:translateY(-2px);text-decoration:none}
.card-title{font-size:1.02em;font-weight:bold;color:#22D4EB;line-height:1.3}
.card-desc{font-size:.83em;color:#9CA3AF;line-height:1.5}
.card-tags{display:flex;flex-wrap:wrap;gap:6px;margin-top:auto;padding-top:4px}
.tag{font-size:.72em;background:#1F2937;border:1px solid #374151;border-radius:4px;padding:2px 8px;color:#6B7280}
#q{width:100%;background:#111827;border:1px solid #2A2A3A;border-radius:8px;color:#E0E0E0;padding:10px 14px;font-size:.95em;margin:14px 0 4px}
#q:focus{outline:none;border-color:#22D4EB}
.sub{color:#9CA3AF;font-size:.9em}
"""


def clean(t, n=260):
    t = re.sub(r"[*_`]+", "", t)
    t = re.sub(r"\[([^\]]+)\]\([^)]*\)", r"\1", t)
    t = re.sub(r"\s+", " ", t).strip()
    if len(t) > n:
        t = t[:n].rsplit(" ", 1)[0].rstrip(",;:-") + "…"
    t = t[:1].upper() + t[1:] if t else t
    return t


def summarize(md):
    lines = md.splitlines()
    for key in ("**Question addressed:**", "**Scope:**"):
        for i, l in enumerate(lines):
            if l.strip().startswith(key):
                buf = [l.strip()[len(key):].strip()]
                j = i + 1
                while j < len(lines) and lines[j].strip() and not lines[j].startswith(("**", "#")):
                    buf.append(lines[j].strip()); j += 1
                return clean(" ".join(buf))
    heads = [re.sub(r"^\d+[.)]\s*", "", l[3:].strip()) for l in lines if l.startswith("## ")]
    heads = [h for h in heads if not re.match(r"(earlier in session|background|summary|contents)", h, re.I)]
    return clean(" · ".join(heads[:4])) if heads else ""


def render(md_path):
    r = subprocess.run([PANDOC, "-f", "gfm", "-t", "html5", "--wrap=none", "--no-highlight", md_path],
                       capture_output=True, text=True, check=True)
    return r.stdout


def copy_image(src_dir, ref):
    p = os.path.normpath(os.path.join(src_dir, ref))
    if not os.path.isfile(p):
        return None
    base = os.path.basename(p)
    dest = base
    if os.path.exists(os.path.join(IMG, dest)):
        same = hashlib.md5(open(p, "rb").read()).hexdigest() == hashlib.md5(open(os.path.join(IMG, dest), "rb").read()).hexdigest()
        if not same:
            dest = hashlib.md5(p.encode()).hexdigest()[:8] + "_" + base
    shutil.copyfile(p, os.path.join(IMG, dest))
    return "img/" + dest


def page(title, badge, body, nav_extra=""):
    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="robots" content="noindex,nofollow"><title>{html.escape(title)}</title><style>{CSS}{BOARD_CSS}</style></head>
<body><div class="wrap">
<div class="nav"><a href="index.html">&larr; Daily reports</a><a href="../index.html">Hub</a>{nav_extra}</div>
<h1>{html.escape(title)}</h1><div class="meta">{badge}</div>
<div class="article">{body}</div>
<div class="footer">FluxNet &times; Mortality &middot; generated from daily_reports/*.md</div>
</div></body></html>"""


def main():
    # OUT is entirely generated: clear old pages/images first so a newly excluded report can never
    # linger and get published by the push script's rsync --delete mirror
    if os.path.isdir(OUT):
        for f in os.listdir(OUT):
            if f.endswith(".html"):
                os.remove(os.path.join(OUT, f))
        shutil.rmtree(IMG, ignore_errors=True)
    os.makedirs(IMG, exist_ok=True)
    reports = []
    stems = {os.path.splitext(f)[0] for f in os.listdir(SRC) if f.endswith(".md") and f not in EXCLUDE}
    for f in sorted(os.listdir(SRC)):
        if not f.endswith(".md") or f in EXCLUDE:
            continue
        stem = os.path.splitext(f)[0]
        md = open(os.path.join(SRC, f), encoding="utf-8").read()
        m = re.search(r"^#\s+(.+)$", md, re.M)
        title = clean(m.group(1), 200) if m else stem
        dm = re.match(r"(\d{4}-\d{2}-\d{2})", f)
        date = dm.group(1) if dm else None
        body = render(os.path.join(SRC, f))
        n_img = body.count("<img")

        def fix_img(mo):
            new = None if re.match(r"https?:", mo.group(1)) else copy_image(SRC, mo.group(1))
            return f'src="{new}" loading="lazy"' if new else mo.group(0)
        body = re.sub(r'src="([^"]+)"', fix_img, body)
        body = re.sub(r'(<img [^>]*src="(img/[^"]+)"[^>]*>)', r'<a href="\2" target="_blank">\1</a>', body)
        body = re.sub(r'href="([^"#]+)\.md(#[^"]*)?"',
                      lambda mo: f'href="{mo.group(1)}.html{mo.group(2) or ""}"' if os.path.basename(mo.group(1)) in stems else mo.group(0), body)
        n_tab = body.count("<table")
        body = body.replace("<table", '<div class="tw"><table').replace("</table>", "</table></div>")
        badge = (f'<span class="badge">{date}</span>' if date else '<span class="badge">Reference</span>')
        if n_img: badge += f'<span class="badge">{n_img} figure{"s" if n_img != 1 else ""}</span>'
        if n_tab: badge += f'<span class="badge">{n_tab} table{"s" if n_tab != 1 else ""}</span>'
        open(os.path.join(OUT, stem + ".html"), "w", encoding="utf-8").write(page(title, badge, body))
        reports.append(dict(stem=stem, title=title, date=date, summary=summarize(md), n_img=n_img, n_tab=n_tab))

    reports.sort(key=lambda r: (r["date"] is None, r["date"] or ""), reverse=False)
    dated = sorted([r for r in reports if r["date"]], key=lambda r: r["date"], reverse=True)
    undated = [r for r in reports if not r["date"]]

    def card(r):
        tags = (f'<span class="tag">{r["date"]}</span>' if r["date"] else '<span class="tag">Reference</span>')
        if r["n_img"]: tags += f'<span class="tag">{r["n_img"]} figure{"s" if r["n_img"] != 1 else ""}</span>'
        if r["n_tab"]: tags += f'<span class="tag">{r["n_tab"]} table{"s" if r["n_tab"] != 1 else ""}</span>'
        hay = html.escape((r["title"] + " " + r["summary"]).lower(), quote=True)
        return (f'<a class="card" href="{r["stem"]}.html" data-h="{hay}"><div class="card-title">{html.escape(r["title"])}</div>'
                f'<div class="card-desc">{html.escape(r["summary"])}</div><div class="card-tags">{tags}</div></a>')

    blocks, cur = [], None
    for r in dated:
        month = datetime.date.fromisoformat(r["date"]).strftime("%B %Y")
        if month != cur:
            if cur is not None: blocks.append("</div></section>")
            blocks.append(f'<section><div class="month">{month}</div><div class="cards">'); cur = month
        blocks.append(card(r))
    if cur is not None: blocks.append("</div></section>")
    if undated:
        blocks.append('<section><div class="month">Reference</div><div class="cards">' + "".join(card(r) for r in undated) + "</div></section>")

    board_body = f"""<p class="sub">{len(reports)} reports, newest first. Methods, decisions and results, with figures and tables.</p>
<input id="q" placeholder="Filter reports (title or summary)..." autocomplete="off">
{''.join(blocks)}
<script>
const q=document.getElementById('q');
q.addEventListener('input',()=>{{const v=q.value.toLowerCase().trim();
document.querySelectorAll('.card').forEach(c=>{{c.style.display=c.dataset.h.includes(v)?'':'none'}});
document.querySelectorAll('section').forEach(s=>{{s.style.display=[...s.querySelectorAll('.card')].some(c=>c.style.display!=='none')?'':'none'}});}});
</script>"""
    board = page("Daily Reports", "", board_body).replace('<a href="index.html">&larr; Daily reports</a>', "")
    board = board.replace('<div class="article">', '<div class="article" style="margin-top:0">')
    open(os.path.join(OUT, "index.html"), "w", encoding="utf-8").write(board)

    hub = os.path.join(SITE, "index.html")
    h = open(hub, encoding="utf-8").read()
    if 'href="daily_reports/index.html"' not in h:
        card_html = """  <a class="card" href="daily_reports/index.html">
    <div class="card-icon">📝</div>
    <div class="card-title">Daily Reports</div>
    <div class="card-desc">
      Running notebook of the analysis: model structures, reporting decisions, permutation and
      circularity tests, data-quality checks. Newest first, with the figures and tables.
    </div>
    <div class="card-tags">
      <span class="tag">Methods</span>
      <span class="tag">Decisions</span>
      <span class="tag">Figures</span>
    </div>
  </a>

"""
        marker = '\n</div>\n\n<div class="footer">'
        if marker not in h:
            sys.exit("hub marker not found - hub page layout changed, add the card by hand")
        h = h.replace(marker, "\n" + card_html.rstrip("\n") + "\n" + marker, 1)
        open(hub, "w", encoding="utf-8").write(h)
        print("hub: added Daily Reports card")
    else:
        print("hub: card already present")
    print(f"built {len(reports)} report pages + board in {OUT}; images in {IMG}: {len(os.listdir(IMG))}")


if __name__ == "__main__":
    main()
