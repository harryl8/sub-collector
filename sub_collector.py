#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
sub_collector.py — 每日从 GitHub 采集最近更新的免费 V2Ray / Clash 订阅仓库，
提取 all / 综合订阅文件的 raw 链接，做轻量内容校验后输出清单。

设计说明
- 只用 Python 3 标准库，无需 pip 安装任何东西。
- 数据来源：GitHub 官方 API（search + git trees + raw 文件），不是网页抓取。
- “校验”只确认链接活着且内容像订阅（能数出节点条数），不承诺每个节点可用。
- SS 过滤：纯 SS 的订阅文件整条剔除；混合文件保留，但节点数只统计非 SS 节点，
  文件里若一个非 SS 节点都没有，整条链接丢弃。
- 带 state.json 缓存：仓库 pushed_at 没变的直接复用上次结果，省 API 额度。
- 未填 GITHUB_TOKEN 时 GitHub 核心 API 是 60 次/小时；扫到限额会自动停扫、
  用已拿到的结果出清单，不会整轮报废（填了 TOKEN 则限额 5000 次/小时）。

可选环境变量
  GITHUB_TOKEN   填了可大幅提高 GitHub API 限额（不填也能跑，每日一次足够）
  MAX_REPOS      最多深入扫描的仓库数（默认 60）
  DAYS           只看最近多少天内有更新的仓库（默认 7）
  MAX_LINKS      清单最多输出多少条链接（默认 120）

输出（脚本同目录的 out/ 下）
  latest.txt            最新清单：仓库 / 更新时间 / raw 链接 / 约节点数
  all_links.txt         纯链接列表，方便整段复制
  report-YYYY-MM-DD.md  当日完整报告
  state.json            在脚本同目录，跨天缓存（自动维护，别手动删）

用法
  python3 sub_collector.py            # 跑一次
  crontab 每天跑：见部署说明
"""

import base64
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(BASE_DIR, "out")
STATE_FILE = os.path.join(BASE_DIR, "state.json")

TOKEN = os.environ.get("GITHUB_TOKEN", "").strip()
MAX_REPOS = int(os.environ.get("MAX_REPOS", "60"))
DAYS = int(os.environ.get("DAYS", "7"))
MAX_LINKS = int(os.environ.get("MAX_LINKS", "120"))

QUERIES = [
    "v2ray subscription",
    "clash subscription",
    "v2ray free nodes",
    "free v2ray nodes",
    "v2ray 订阅",
    "clash 订阅",
    "免费节点 订阅",
]

SCHEMES = ("vmess://", "vless://", "trojan://", "ssr://",
           "hysteria2://", "hy2://", "tuic://")
SS_SCHEMES = ("ss://",)  # 单独统计，不计入可用节点数
DATE_NAME_RE = re.compile(r"(20\d{2}[-_]?[01]\d[-_]?[0-3]\d)|(\d{8})")
GOOD_EXT = (".txt", ".yaml", ".yml")
KEYWORDS = ("all", "v2ray", "clash", "sub", "node")


def http_get(url, timeout=30, limit=None):
    """GET 并返回 bytes；失败抛异常。limit 为最多读取字节数。"""
    req = urllib.request.Request(url, headers={
        "User-Agent": "sub-collector/1.0",
        "Accept": "application/vnd.github+json",
    })
    if TOKEN:
        req.add_header("Authorization", "Bearer " + TOKEN)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        if limit:
            return resp.read(limit)
        return resp.read()


class RateLimited(Exception):
    """GitHub API 限额用完：停扫并用已有结果出清单。"""


def api_json(url):
    try:
        raw = http_get(url)
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8", "ignore")
        except Exception:
            pass
        if e.code in (403, 429) and "rate limit" in body.lower():
            raise RateLimited("GitHub API rate limit")
        raise
    return json.loads(raw.decode("utf-8", "ignore"))


def search_repos(since_date):
    """多个关键词搜索最近更新的仓库，按 full_name 去重。"""
    found = {}
    for q in QUERIES:
        query = '%s pushed:>%s' % (q, since_date)
        url = ("https://api.github.com/search/repositories?q="
               + urllib.parse.quote(query)
               + "&sort=updated&order=desc&per_page=30")
        try:
            data = api_json(url)
        except RateLimited:
            print("  [warn] GitHub search rate limited; stopping search.",
                  file=sys.stderr)
            break
        except Exception as e:
            print("  [warn] search failed (%s): %s" % (q, e), file=sys.stderr)
            continue
        for item in data.get("items", []):
            found[item["full_name"]] = {
                "full_name": item["full_name"],
                "pushed_at": item.get("pushed_at", ""),
                "branch": item.get("default_branch", "main"),
                "html_url": item.get("html_url", ""),
            }
        if not TOKEN:
            time.sleep(7)  # 未认证 search 限额 10 次/分钟，稳一点
    repos = sorted(found.values(), key=lambda r: r["pushed_at"], reverse=True)
    return repos


def candidate_files(repo):
    """取仓库文件树，挑出像综合订阅的文件（最多 3 个，all 优先）。"""
    url = "https://api.github.com/repos/%s/git/trees/%s?recursive=1" % (
        repo["full_name"], urllib.parse.quote(repo["branch"]))
    data = api_json(url)
    scored = []
    for node in data.get("tree", []):
        if node.get("type") != "blob":
            continue
        path = node["path"]
        low = path.lower()
        name = low.rsplit("/", 1)[-1]
        if not name.endswith(GOOD_EXT):
            continue
        if DATE_NAME_RE.search(name):
            continue  # 跳过按日期命名的文件（如 2026-11-10.txt）
        if "readme" in name or "license" in name or "broken" in name:
            continue  # 仓库作者自己标注 broken 的文件不要
        if re.match(r"ssr?($|[-_.0-9])", name):
            continue  # 纯 SS 的文件（如 ss.txt / ss2022.txt）跳过
        if not any(k in low for k in KEYWORDS):
            continue
        score = 0
        if "all" in name:
            score += 10
        if name.endswith((".yaml", ".yml")):
            score += 3
        if "/" not in path:
            score += 2  # 根目录文件优先
        scored.append((score, path))
    scored.sort(key=lambda x: -x[0])
    return [p for _, p in scored[:5]]


def _count_in_text(text):
    """在一段文本里数节点：返回 (非SS节点数, SS节点数, 格式)；数不出返回 (0,0,'')。"""
    if "proxies:" in text:
        total = text.count("name:")
        ss_n = len(re.findall(r"type:\s*ss\b", text))
        return max(total - ss_n, 0), ss_n, "clash-yaml"
    total = sum(text.count(s) for s in SCHEMES)
    # 注意：不能直接 count("ss://")，"vmess://"/"vless://" 里也含这个子串
    ss_n = len(re.findall(r"(?<![a-zA-Z])ss://", text))
    if total + ss_n > 0:
        return total, ss_n, "plain"
    return 0, 0, ""


def count_nodes(raw_bytes):
    """轻量校验：数内容里的节点条数，SS 单独统计、不计入可用数。
    返回 (非SS节点数, SS节点数, 格式说明)。"""
    text = raw_bytes.decode("utf-8", "ignore")
    n, ss_n, fmt = _count_in_text(text)
    if fmt:
        return n, ss_n, fmt
    # 很多订阅整体是 base64，解开再数一次
    compact = re.sub(r"\s+", "", text)
    try:
        decoded = base64.b64decode(compact + "=" * (-len(compact) % 4),
                                   validate=False).decode("utf-8", "ignore")
        n, ss_n, fmt = _count_in_text(decoded)
        if fmt:
            return n, ss_n, "base64"
    except Exception:
        pass
    return 0, 0, "unknown"


def check_link(repo, path):
    raw_url = "https://raw.githubusercontent.com/%s/%s/%s" % (
        repo["full_name"], repo["branch"],
        urllib.parse.quote(path))
    rec = {"path": path, "url": raw_url, "nodes": 0, "ss_nodes": 0,
           "format": "", "ok": False}
    try:
        data = http_get(raw_url, timeout=25, limit=300_000)
        n, ss_n, fmt = count_nodes(data)
        rec["nodes"] = n
        rec["ss_nodes"] = ss_n
        rec["format"] = fmt
        rec["ok"] = n > 0  # 只有 SS、没有其他协议的，整条丢弃
    except Exception as e:
        rec["format"] = "error: %s" % e
    return rec


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    state = {}
    if os.path.exists(STATE_FILE):
        try:
            state = json.loads(open(STATE_FILE, encoding="utf-8").read())
        except Exception:
            state = {}

    since = (datetime.now(timezone.utc) - timedelta(days=DAYS)).strftime("%Y-%m-%d")
    print("Searching GitHub repos updated since %s ..." % since)
    repos = search_repos(since)
    print("Found %d candidate repos; scanning top %d." % (len(repos), MAX_REPOS))

    results = []   # 每项: {repo, pushed_at, html_url, links:[rec,...]}
    new_state = {}
    hit_limit = False
    for repo in repos[:MAX_REPOS]:
        name = repo["full_name"]
        cached = state.get(name)
        if cached and cached.get("pushed_at") == repo["pushed_at"]:
            entry = cached
            print("  = %s (unchanged, cached)" % name)
        else:
            entry = {"pushed_at": repo["pushed_at"],
                     "html_url": repo["html_url"], "links": []}
            try:
                paths = candidate_files(repo)
            except RateLimited:
                print("  ! GitHub API rate limit reached; stopping scan "
                      "early, keeping results so far.", file=sys.stderr)
                hit_limit = True
                paths = []
            except Exception as e:
                print("  ! %s tree failed: %s" % (name, e), file=sys.stderr)
                paths = []
            for p in paths:
                entry["links"].append(check_link(repo, p))
            ok_n = sum(1 for l in entry["links"] if l["ok"])
            print("  + %s: %d files, %d ok" % (name, len(paths), ok_n))
        new_state[name] = entry
        if hit_limit:
            break
        good = [l for l in entry["links"] if l["ok"]]
        if good:
            results.append({"repo": name, "pushed_at": repo["pushed_at"],
                            "html_url": repo["html_url"], "links": good})

    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(new_state, f, ensure_ascii=False, indent=1)

    # 汇总输出
    today = datetime.now().strftime("%Y-%m-%d")
    lines, urls, md = [], [], ["# 订阅采集报告 %s" % today, ""]
    total = 0
    for r in results:
        date = r["pushed_at"][:10]
        for l in r["links"]:
            if total >= MAX_LINKS:
                break
            total += 1
            lines.append("%s | %s | %s | ~%d nodes, SS excluded (%s)" % (
                r["repo"], date, l["url"], l["nodes"], l["format"]))
            urls.append(l["url"])
            ss_note = "，另有 SS %d 个未计入" % l["ss_nodes"] if l.get("ss_nodes") else ""
            md.append("- **%s**（%s）[%s](%s) — 约 %d 个节点（%s%s）" % (
                r["repo"], date, l["path"], l["url"], l["nodes"], l["format"],
                ss_note))
    header = "# %s | 共 %d 条可用订阅链接（最近 %d 天更新的仓库，节点数不含 SS）" % (
        today, total, DAYS)
    with open(os.path.join(OUT_DIR, "latest.txt"), "w", encoding="utf-8") as f:
        f.write(header + "\n\n" + "\n".join(lines) + "\n")
    with open(os.path.join(OUT_DIR, "all_links.txt"), "w", encoding="utf-8") as f:
        f.write("\n".join(urls) + ("\n" if urls else ""))
    with open(os.path.join(OUT_DIR, "report-%s.md" % today), "w",
              encoding="utf-8") as f:
        f.write("\n".join(md) + "\n")

    print("\nDone: %d links from %d repos -> out/latest.txt" % (total, len(results)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
