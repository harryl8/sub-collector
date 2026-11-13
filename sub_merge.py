#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
sub_merge.py — 把 sub_collector.py 采集到的订阅链接全部抓下来，提取节点、
去重、剔除 SS，输出两份合并订阅（由现有的 HTTP 服务直接对外提供）：

  out/merged-v2ray.txt        base64 编码的 URI 合并订阅（v2rayN / 通用格式）
  out/merged-v2ray-plain.txt  同上明文版（排查用）
  out/merged-clash.yaml       Clash 合并配置（proxies + 一个选择组 + MATCH 规则）
  out/merge-stats.txt         本次合并统计

去重口径：按（协议 + 服务器 + 端口 + 凭证）判重，不看备注名 — 同一节点在不同
订阅里换个名字也只留一份。
存活检查分两级：
一级（粗筛）：去重后按（服务器 + 端口）端点做并发 TCP 连通检测，同一端点
只测一次，测不通的整端点节点全部剔除。hysteria2 / tuic 是 UDP 协议，TCP
测不准，跳过一级检查、直接进二级。CHECK_ALIVE=0 可关闭一级检查。
二级（真实测速，默认开）：一级过滤后的候选节点全量加载进 mihomo 内核，
逐个真的经节点访问测试 URL（generate_204），只有拿到响应才算通过；通过
的按延迟排序，再按“同一服务器最多 PER_IP_CAP 个”限额后取舍 — 发布出去
的每个节点都是发布前几分钟刚实测可用的。mihomo 二进制不存在时脚本会
尝试从 GitHub 自动下载；下载失败或内核不可用时自动回退到只用一级检查
的旧行为，绝不中断合并。REAL_TEST=0 可关闭二级检查。
输出裁剪：真实测速开启时按延迟与服务器限额选拔（见下文）；关闭时沿用
旧逻辑 — 存活过滤后按来源轮询交错取前 MAX_NODES 个（默认 200），避免
排在前面的订阅吃光名额。
Clash 规则：运行时拉取 ACL4SSR_INI 指向的 ini（默认 ACL4SSR 官方仓库的
Mini 版），按其中的 custom_proxy_group 生成策略组、ruleset 生成
rule-providers + 规则；拉取失败则回退到单个选择组 + MATCH。
SS 口径：ss:// 节点与 Clash 里 type: ss 的条目一律剔除（沿用采集阶段的要求）；
SSR 不在剔除范围。
来源口径：URI 订阅（明文 / base64）里的节点同时进两份输出；Clash YAML 订阅
里的节点只进 clash 版，所以 clash 版通常比 v2ray 版多。YAML 来源的条目一律
先解析成字段、按白名单重组、过合法性校验（类型/服务器/端口/UUID 格式等）
才输出，绝不原文透传 — 免费仓库里混着的示例模板、占位符、坏缩进条目会在
这一层被剔除（计入 bad_skipped / clash_bad）。

只用 Python 3 标准库。可选环境变量：
  LINKS_FILE     订阅链接清单（默认脚本同目录 out/all_links.txt）
  OUT_DIR        输出目录（默认与 LINKS_FILE 同目录）
  MAX_NODES      每份输出的节点上限（默认 200）
  ACL4SSR_INI    Clash 规则 ini 地址（默认 ACL4SSR 官方 Mini 版，可指向自己的 fork）
  CHECK_ALIVE    存活检查开关（默认 1 开；0 关）
  CHECK_TIMEOUT  单个端点的连接超时秒数（默认 3）
  CHECK_WORKERS  存活检查并发数（默认 100）
  REAL_TEST      真实测速开关（默认 1 开；0 关，回退到一级检查旧行为）
  REAL_CAP       真实测速最多测多少个候选节点（默认 4000，交错顺序在前优先）
  REAL_TIMEOUT_MS 单节点测速超时毫秒（默认 6000）
  REAL_DEADLINE  真实测速总时长上限秒数（默认 900，超时用已通过的结果出货）
  PER_IP_CAP     最终输出里同一服务器最多留几个节点（默认 3）
  MIHOMO_BIN     mihomo 二进制路径（默认脚本同目录下的 mihomo）
"""

import base64
import gzip
import hashlib
import json
import os
import platform
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.parse
import urllib.request
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LINKS_FILE = os.environ.get(
    "LINKS_FILE", os.path.join(BASE_DIR, "out", "all_links.txt"))
OUT_DIR = os.environ.get(
    "OUT_DIR", os.path.dirname(os.path.abspath(LINKS_FILE)) or ".")
MAX_NODES = int(os.environ.get("MAX_NODES", "200"))
INI_URL = os.environ.get(
    "ACL4SSR_INI",
    "https://raw.githubusercontent.com/ACL4SSR/ACL4SSR/master/Clash/config/"
    "ACL4SSR_Mini.ini")
FETCH_LIMIT = 3_000_000
CHECK_ALIVE = os.environ.get("CHECK_ALIVE", "1") not in ("0", "false", "no")
CHECK_TIMEOUT = float(os.environ.get("CHECK_TIMEOUT", "3"))
CHECK_WORKERS = int(os.environ.get("CHECK_WORKERS", "100"))
REAL_TEST = os.environ.get("REAL_TEST", "1") not in ("0", "false", "no")
REAL_CAP = int(os.environ.get("REAL_CAP", "4000"))
REAL_CHUNK = int(os.environ.get("REAL_CHUNK", "400"))
REAL_WORKERS = int(os.environ.get("REAL_WORKERS", "40"))
REAL_TIMEOUT_MS = int(os.environ.get("REAL_TIMEOUT_MS", "6000"))
REAL_DEADLINE = float(os.environ.get("REAL_DEADLINE", "900"))
PER_IP_CAP = int(os.environ.get("PER_IP_CAP", "3"))
MIHOMO_BIN = os.environ.get(
    "MIHOMO_BIN",
    os.path.join(BASE_DIR, "mihomo"))
MIHOMO_CTRL_PORT = 19327
MIHOMO_MIXED_PORT = 17827
TEST_URL = "http://www.gstatic.com/generate_204"

URI_RE = re.compile(
    r"(?:vmess|vless|trojan|ssr|ss|hysteria2|hy2|tuic)://[^\s\"'<>`]+")

GROUP_NAME = "合并节点"


# ---------- 基础工具 ----------

def http_get(url, timeout=25):
    req = urllib.request.Request(url, headers={"User-Agent": "sub-merge/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read(FETCH_LIMIT)


def b64d(s):
    s = re.sub(r"\s+", "", s).replace("-", "+").replace("_", "/")
    return base64.b64decode(s + "=" * (-len(s) % 4)).decode("utf-8", "ignore")


def clean_name(name, fallback):
    name = re.sub(r"[\r\n\t]+", " ", (name or "")).strip()
    return name or fallback


class NamePool:
    """保证 Clash 里节点名唯一，并记录分配顺序。"""

    def __init__(self):
        self.used = set()
        self.order = []

    def take(self, name):
        base = name
        n = 2
        while name in self.used:
            name = "%s-%d" % (base, n)
            n += 1
        self.used.add(name)
        self.order.append(name)
        return name


def yaml_flow(d):
    """把 dict 输出成 YAML 流式映射（字符串用 JSON 引号，YAML 可直接解析）。"""
    parts = []
    for k, v in d.items():
        if isinstance(v, bool):
            parts.append("%s: %s" % (k, "true" if v else "false"))
        elif isinstance(v, (int, float)):
            parts.append("%s: %s" % (k, v))
        elif isinstance(v, dict):
            parts.append("%s: %s" % (k, yaml_flow(v)))
        elif isinstance(v, list):
            inner = ", ".join(
                json.dumps(x, ensure_ascii=False) if isinstance(x, str)
                else str(x) for x in v)
            parts.append("%s: [%s]" % (k, inner))
        else:
            parts.append("%s: %s" % (k, json.dumps(str(v), ensure_ascii=False)))
    return "{" + ", ".join(parts) + "}"


# ---------- URI 解析：返回 (去重键, 备注名, clash dict 或 None) ----------

def parse_vmess(uri):
    payload = uri[len("vmess://"):]
    try:
        j = json.loads(b64d(payload))
        server = j.get("add", "")
        port = int(j.get("port", 0))
        uuid = j.get("id", "")
        name = clean_name(j.get("ps"), "vmess-%s-%s" % (server, port))
        d = {"name": name, "type": "vmess", "server": server, "port": port,
             "uuid": uuid, "alterId": int(j.get("aid", 0) or 0),
             "cipher": j.get("scy") or "auto", "udp": True}
        net = j.get("net") or "tcp"
        if net and net != "tcp":
            d["network"] = net
        if j.get("tls") == "tls":
            d["tls"] = True
            if j.get("sni"):
                d["servername"] = j["sni"]
        if net == "ws":
            opts = {}
            if j.get("path"):
                opts["path"] = j["path"]
            if j.get("host"):
                opts["headers"] = {"Host": j["host"]}
            if opts:
                d["ws-opts"] = opts
        elif net == "grpc" and j.get("path"):
            d["grpc-opts"] = {"grpc-service-name": j["path"]}
        return ("vmess", server, port, uuid), name, d
    except Exception:
        pass
    # 新式 vmess://uuid@host:port?... 写法
    try:
        u = urllib.parse.urlsplit(uri)
        server = u.hostname or ""
        port = u.port or 0
        uuid = urllib.parse.unquote(u.username or "")
        q = urllib.parse.parse_qs(u.query)
        name = clean_name(urllib.parse.unquote(u.fragment),
                          "vmess-%s-%s" % (server, port))
        d = {"name": name, "type": "vmess", "server": server, "port": port,
             "uuid": uuid, "alterId": 0, "cipher": "auto", "udp": True}
        if q.get("security", [""])[0] in ("tls", "reality"):
            d["tls"] = True
        return ("vmess", server, port, uuid), name, d
    except Exception:
        return None


def _net_opts(d, q):
    """ws / grpc 的公共参数映射（vless / trojan 用）。"""
    net = q.get("type", ["tcp"])[0]
    if net and net != "tcp":
        d["network"] = net
    if net == "ws":
        opts = {}
        if q.get("path"):
            opts["path"] = q["path"][0]
        if q.get("host"):
            opts["headers"] = {"Host": q["host"][0]}
        if opts:
            d["ws-opts"] = opts
    elif net == "grpc" and q.get("serviceName"):
        d["grpc-opts"] = {"grpc-service-name": q["serviceName"][0]}


def parse_vless(uri):
    try:
        u = urllib.parse.urlsplit(uri)
        server = u.hostname or ""
        port = u.port or 0
        uuid = urllib.parse.unquote(u.username or "")
        q = urllib.parse.parse_qs(u.query)
        name = clean_name(urllib.parse.unquote(u.fragment),
                          "vless-%s-%s" % (server, port))
        d = {"name": name, "type": "vless", "server": server, "port": port,
             "uuid": uuid, "udp": True}
        security = q.get("security", [""])[0]
        if security in ("tls", "reality"):
            d["tls"] = True
        if q.get("sni"):
            d["servername"] = q["sni"][0]
        if q.get("flow"):
            d["flow"] = q["flow"][0]
        if q.get("fp"):
            d["client-fingerprint"] = q["fp"][0]
        if security == "reality":
            ro = {}
            if q.get("pbk"):
                ro["public-key"] = q["pbk"][0]
            if q.get("sid"):
                ro["short-id"] = q["sid"][0]
            if ro:
                d["reality-opts"] = ro
        _net_opts(d, q)
        return ("vless", server, port, uuid), name, d
    except Exception:
        return None


def parse_trojan(uri):
    try:
        u = urllib.parse.urlsplit(uri)
        server = u.hostname or ""
        port = u.port or 0
        password = urllib.parse.unquote(u.username or "")
        q = urllib.parse.parse_qs(u.query)
        name = clean_name(urllib.parse.unquote(u.fragment),
                          "trojan-%s-%s" % (server, port))
        d = {"name": name, "type": "trojan", "server": server, "port": port,
             "password": password, "udp": True}
        if q.get("sni"):
            d["sni"] = q["sni"][0]
        _net_opts(d, q)
        return ("trojan", server, port, password), name, d
    except Exception:
        return None


def parse_ssr(uri):
    try:
        dec = b64d(uri[len("ssr://"):])
        main, _, query = dec.partition("/?")
        parts = main.rsplit(":", 5)
        if len(parts) != 6:
            return None
        server, port, protocol, method, obfs, pwd_b64 = parts
        port = int(port)
        password = b64d(pwd_b64)
        q = urllib.parse.parse_qs(query)
        remarks = b64d(q.get("remarks", [""])[0]) if q.get("remarks") else ""
        name = clean_name(remarks, "ssr-%s-%s" % (server, port))
        d = {"name": name, "type": "ssr", "server": server, "port": port,
             "cipher": method, "password": password,
             "protocol": protocol, "obfs": obfs, "udp": True}
        if q.get("protoparam"):
            d["protocol-param"] = b64d(q["protoparam"][0])
        if q.get("obfsparam"):
            d["obfs-param"] = b64d(q["obfsparam"][0])
        return ("ssr", server, port, password), name, d
    except Exception:
        return None


def parse_hysteria2(uri):
    try:
        u = urllib.parse.urlsplit(uri)
        server = u.hostname or ""
        port = u.port or 0
        auth = urllib.parse.unquote(u.username or "")
        if u.password:
            auth = auth + ":" + urllib.parse.unquote(u.password)
        q = urllib.parse.parse_qs(u.query)
        name = clean_name(urllib.parse.unquote(u.fragment),
                          "hy2-%s-%s" % (server, port))
        d = {"name": name, "type": "hysteria2", "server": server,
             "port": port, "password": auth, "udp": True}
        if q.get("sni"):
            d["sni"] = q["sni"][0]
        if q.get("insecure", [""])[0] in ("1", "true"):
            d["skip-cert-verify"] = True
        if q.get("obfs"):
            d["obfs"] = q["obfs"][0]
            if q.get("obfs-password"):
                d["obfs-password"] = q["obfs-password"][0]
        return ("hysteria2", server, port, auth), name, d
    except Exception:
        return None


def parse_tuic(uri):
    try:
        u = urllib.parse.urlsplit(uri)
        server = u.hostname or ""
        port = u.port or 0
        uuid = urllib.parse.unquote(u.username or "")
        password = urllib.parse.unquote(u.password or "")
        q = urllib.parse.parse_qs(u.query)
        name = clean_name(urllib.parse.unquote(u.fragment),
                          "tuic-%s-%s" % (server, port))
        d = {"name": name, "type": "tuic", "server": server, "port": port,
             "uuid": uuid, "password": password, "udp": True}
        if q.get("sni"):
            d["sni"] = q["sni"][0]
        if q.get("alpn"):
            d["alpn"] = [q["alpn"][0]]
        return ("tuic", server, port, uuid), name, d
    except Exception:
        return None


PARSERS = {
    "vmess": parse_vmess, "vless": parse_vless, "trojan": parse_trojan,
    "ssr": parse_ssr, "hysteria2": parse_hysteria2, "hy2": parse_hysteria2,
    "tuic": parse_tuic,
}


# ---------- Clash YAML 来源：提取 proxies 条目 ----------

def extract_yaml_proxies(text):
    """从 Clash 配置文本里提取 proxies: 下的条目（原始行组）。"""
    lines = text.splitlines()
    start = None
    for i, ln in enumerate(lines):
        if re.match(r"^proxies\s*:", ln):
            start = i
            break
    if start is None:
        return []
    entries, cur, base_indent = [], None, None
    for ln in lines[start + 1:]:
        if re.match(r"^\S", ln):  # 下一个顶层键，结束
            break
        m = re.match(r"^(\s*)-\s", ln)
        if m and (base_indent is None or len(m.group(1)) == base_indent):
            if base_indent is None:
                base_indent = len(m.group(1))
            if cur is not None:
                entries.append(cur)
            cur = [ln]
        elif cur is not None:
            cur.append(ln)
    if cur is not None:
        entries.append(cur)
    return entries


UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{12}$")
SUPPORTED_TYPES = ("vmess", "vless", "trojan", "ssr", "hysteria",
                   "hysteria2", "tuic", "http", "socks5")
SSR_PROTOCOLS = ("origin", "verify_sha1", "auth_sha1_v4", "auth_aes128_md5",
                 "auth_aes128_sha1", "auth_chain_a", "auth_chain_b")
SSR_OBFS = ("plain", "http_simple", "http_post", "random_head",
            "tls1.2_ticket_auth", "tls1.2_ticket_fastauth")
PLACEHOLDER_SERVERS = ("", "server.com", "example.com", "yourpassword",
                       "localhost")


def _dq_unescape(s):
    out, i = [], 0
    simple = {"n": "\n", "t": "\t", "r": "\r", '"': '"', "\\": "\\", "/": "/"}
    while i < len(s):
        c = s[i]
        if c == "\\" and i + 1 < len(s):
            n = s[i + 1]
            try:
                if n in simple:
                    out.append(simple[n]); i += 2; continue
                if n == "x":
                    out.append(chr(int(s[i + 2:i + 4], 16))); i += 4; continue
                if n == "u":
                    out.append(chr(int(s[i + 2:i + 6], 16))); i += 6; continue
                if n == "U":
                    out.append(chr(int(s[i + 2:i + 10], 16))); i += 10
                    continue
            except (ValueError, OverflowError):
                pass
            out.append(n); i += 2; continue
        out.append(c); i += 1
    return "".join(out)


def _scalar(token):
    """YAML 标量 → Python 值（子集）：引号字符串、布尔、null、整数、浮点。"""
    token = token.strip()
    if len(token) >= 2 and token[0] == '"' and token[-1] == '"':
        return _dq_unescape(token[1:-1])
    if len(token) >= 2 and token[0] == "'" and token[-1] == "'":
        return token[1:-1].replace("''", "'")
    if token.startswith("{") or token.startswith("["):
        return _flow_parse(token)
    low = token.lower()
    if low in ("null", "~", "none"):
        return token if low == "none" else None
    if low in ("true", "yes", "on"):
        return True
    if low in ("false", "no", "off"):
        return False
    try:
        return int(token)
    except ValueError:
        pass
    try:
        return float(token)
    except ValueError:
        pass
    return token


def _flow_parse(s):
    """YAML 流式映射/列表的子集解析器（引号、嵌套 {} []、裸标量）。"""
    s = s.strip()
    state = {"i": 0}

    def peek():
        return s[state["i"]] if state["i"] < len(s) else ""

    def skip_ws():
        while state["i"] < len(s) and s[state["i"]] in " \t\r\n":
            state["i"] += 1

    def parse_val(key_pos=False):
        skip_ws()
        c = peek()
        if c == "{":
            return parse_map()
        if c == "[":
            return parse_list()
        if c == '"':
            return parse_q('"')
        if c == "'":
            return parse_q("'")
        return parse_bare(key_pos)

    def parse_map():
        state["i"] += 1
        out = {}
        while True:
            skip_ws()
            if peek() == "}":
                state["i"] += 1
                return out
            k = parse_val(key_pos=True)
            skip_ws()
            if peek() != ":":
                raise ValueError("flow map: expected ':'")
            state["i"] += 1
            out[str(k)] = parse_val()
            skip_ws()
            if peek() == ",":
                state["i"] += 1
                continue
            if peek() == "}":
                state["i"] += 1
                return out
            raise ValueError("flow map: expected ',' or '}'")

    def parse_list():
        state["i"] += 1
        out = []
        while True:
            skip_ws()
            if peek() == "]":
                state["i"] += 1
                return out
            out.append(parse_val())
            skip_ws()
            if peek() == ",":
                state["i"] += 1
                continue
            if peek() == "]":
                state["i"] += 1
                return out
            raise ValueError("flow list: expected ',' or ']'")

    def parse_q(q):
        state["i"] += 1
        buf = []
        while state["i"] < len(s):
            c = s[state["i"]]
            if q == '"' and c == "\\" and state["i"] + 1 < len(s):
                buf.append(c)
                buf.append(s[state["i"] + 1])
                state["i"] += 2
                continue
            if c == q:
                if q == "'" and state["i"] + 1 < len(s) \
                        and s[state["i"] + 1] == "'":
                    buf.append("''")
                    state["i"] += 2
                    continue
                state["i"] += 1
                raw = "".join(buf)
                return _dq_unescape(raw) if q == '"' else \
                    raw.replace("''", "'")
            buf.append(c)
            state["i"] += 1
        raise ValueError("unterminated quote")

    def parse_bare(key_pos):
        start = state["i"]
        while state["i"] < len(s):
            c = s[state["i"]]
            if c in ",}]":
                break
            if c == ":" and key_pos:
                break
            if c == ":" and state["i"] + 1 < len(s) \
                    and s[state["i"] + 1] in " ,}]":
                break
            state["i"] += 1
        return _scalar(s[start:state["i"]])

    v = parse_val()
    skip_ws()
    if state["i"] != len(s):
        raise ValueError("flow: trailing characters")
    return v


def _strip_comment(line):
    in_s = in_d = False
    for i, c in enumerate(line):
        if c == "'" and not in_d:
            in_s = not in_s
        elif c == '"' and not in_s and (i == 0 or line[i - 1] != "\\"):
            in_d = not in_d
        elif c == "#" and not in_s and not in_d \
                and (i == 0 or line[i - 1] in " \t"):
            return line[:i].rstrip()
    return line.rstrip()


def _parse_block(seq, i, indent):
    """块式 YAML 子集递归解析：seq 是 (indent, content) 列表。"""
    if seq[i][1].startswith("- ") or seq[i][1] == "-":
        out = []
        while i < len(seq) and seq[i][0] == indent \
                and (seq[i][1].startswith("- ") or seq[i][1] == "-"):
            content = seq[i][1][2:] if seq[i][1].startswith("- ") else ""
            i += 1
            if content:
                out.append(_scalar(content))
            elif i < len(seq) and seq[i][0] > indent:
                v, i = _parse_block(seq, i, seq[i][0])
                out.append(v)
            else:
                out.append(None)
        return out, i
    out = {}
    while i < len(seq) and seq[i][0] == indent:
        content = seq[i][1]
        m = re.match(r"^([^:]+):\s*(.*)$", content)
        if not m:
            raise ValueError("bad block line: %r" % content)
        key = str(_scalar(m.group(1))) if m.group(1)[:1] in "\"'" \
            else m.group(1).strip()
        rest = m.group(2)
        i += 1
        if rest:
            out[key] = _scalar(rest)
        elif i < len(seq) and seq[i][0] > indent:
            v, i = _parse_block(seq, i, seq[i][0])
            out[key] = v
        else:
            out[key] = None
    return out, i


def parse_block_entry(entry_lines):
    """把一个块式 proxies 条目解析成 dict；不符合子集就抛 ValueError。"""
    first_text = entry_lines[0].lstrip()
    if first_text.startswith("- {") or first_text == "- {":
        joined = " ".join(
            ln.strip() for ln in entry_lines)
        return _flow_parse(joined[joined.index("{"):])
    items = []
    for raw in entry_lines:
        ln = _strip_comment(raw)
        if not ln.strip():
            continue
        items.append((len(ln) - len(ln.lstrip(" ")), ln.strip()))
    if not items:
        raise ValueError("empty entry")
    d0, first = items[0]
    if first.startswith("- "):
        first = first[2:]
    elif first == "-":
        first = ""
    else:
        raise ValueError("entry must start with '-'")
    seq = ([(d0 + 2, first)] if first else []) + items[1:]
    if not seq:
        raise ValueError("empty entry")
    obj, nxt = _parse_block(seq, 0, seq[0][0])
    if nxt != len(seq):
        raise ValueError("trailing block content")
    if not isinstance(obj, dict):
        raise ValueError("entry is not a mapping")
    return obj


def parse_flow_entry(entry_lines):
    joined = " ".join(ln.strip() for ln in entry_lines)
    return _flow_parse(joined[joined.index("{"):])


COMMON_KEYS = ("name", "server", "port", "type", "udp", "tls",
               "skip-cert-verify", "sni", "servername", "alpn")
TYPE_KEYS = {
    "vmess": ("uuid", "alterId", "cipher", "network", "ws-opts",
              "grpc-opts", "h2-opts"),
    "vless": ("uuid", "flow", "network", "ws-opts", "grpc-opts",
              "reality-opts", "client-fingerprint"),
    "trojan": ("password", "network", "ws-opts", "grpc-opts"),
    "ssr": ("cipher", "password", "protocol", "obfs", "protocol-param",
            "obfs-param"),
    "hysteria": ("auth-str", "auth", "protocol", "up", "down", "obfs"),
    "hysteria2": ("password", "obfs", "obfs-password"),
    "tuic": ("uuid", "password", "congestion-controller", "udp-relay-mode",
             "disable-sni"),
    "http": ("username", "password"),
    "socks5": ("username", "password"),
}
OPTS_KEYS = {"ws-opts": ("path", "headers", "max-early-data",
                         "early-data-header-name"),
             "grpc-opts": ("grpc-service-name",),
             "reality-opts": ("public-key", "short-id"),
             "h2-opts": ("host", "path")}


def convert_yaml_proxy(d):
    """字段白名单 + 类型强制转换：只留 Clash 认识的字段，其余一律丢弃。"""
    if not isinstance(d, dict):
        return None
    typ = str(d.get("type") or "").lower()
    if typ not in TYPE_KEYS:
        return None
    out = {}
    for k in COMMON_KEYS + TYPE_KEYS[typ]:
        if k not in d or d[k] is None:
            continue
        v = d[k]
        if k in OPTS_KEYS and isinstance(v, dict):
            sub = {}
            for sk in OPTS_KEYS[k]:
                if sk in v and v[sk] is not None:
                    sub[sk] = v[sk]
            if k == "ws-opts" and isinstance(v.get("headers"), dict):
                sub["headers"] = {str(hk): str(hv) for hk, hv
                                  in v["headers"].items()}
            if sub:
                out[k] = sub
            continue
        out[k] = v
    out["type"] = typ
    # 端口/alterId 强制成整数
    try:
        out["port"] = int(str(out.get("port")).strip())
    except (TypeError, ValueError):
        out["port"] = 0
    if "alterId" in out:
        try:
            out["alterId"] = int(out["alterId"])
        except (TypeError, ValueError):
            out["alterId"] = 0
    if typ == "hysteria" and "auth-str" not in out and out.get("auth"):
        out["auth-str"] = out.pop("auth")
    elif "auth" in out:
        out.pop("auth", None)
    return out


def validate_proxy(d):
    """Clash 可用性校验：返回修正后的 dict，不合格返回 None。
    vless 带 flow 但没开 tls 时摘掉 flow 保留节点，其余硬伤直接判死。"""
    if not d:
        return None
    d = dict(d)
    if d.get("type") not in SUPPORTED_TYPES:
        return None
    server = d.get("server")
    if not isinstance(server, str) or not server.strip() \
            or server.strip().lower() in PLACEHOLDER_SERVERS \
            or any(c in server for c in " \t/"):
        return None
    port = d.get("port")
    if isinstance(port, bool) or not isinstance(port, int) \
            or not (1 <= port <= 65535):
        return None
    if d["type"] in ("vmess", "vless"):
        if not UUID_RE.match(str(d.get("uuid") or "")):
            return None
    if d["type"] == "vless" and d.get("flow") and not d.get("tls"):
        d.pop("flow", None)
    if d["type"] == "ssr":
        if d.get("protocol") not in SSR_PROTOCOLS \
                or d.get("obfs") not in SSR_OBFS \
                or not d.get("cipher") or not d.get("password"):
            return None
    if d["type"] == "hysteria" and not d.get("auth-str"):
        return None
    return d


# ---------- mihomo 真实测速 ----------

def _mihomo_ok(path):
    try:
        r = subprocess.run([path, "-v"], capture_output=True, timeout=10)
        return r.returncode == 0
    except Exception:
        return False


def ensure_mihomo():
    """确保 mihomo 二进制可用：本地有且能跑就用；否则从 GitHub 拉最新版。
    都不行返回 None，调用方回退到一级检查旧行为。"""
    if os.path.isfile(MIHOMO_BIN) and _mihomo_ok(MIHOMO_BIN):
        return MIHOMO_BIN
    arch = {"x86_64": "amd64", "amd64": "amd64",
            "aarch64": "arm64", "arm64": "arm64"}.get(
                platform.machine().lower())
    if not arch:
        return None
    try:
        req = urllib.request.Request(
            "https://api.github.com/repos/MetaCubeX/mihomo/releases/latest",
            headers={"User-Agent": "sub_merge"})
        with urllib.request.urlopen(req, timeout=30) as r:
            rel = json.load(r)
        url = None
        for a in rel.get("assets", []):
            nm = a.get("name", "")
            if nm.startswith("mihomo-linux-%s-v" % arch) \
                    and nm.endswith(".gz"):
                url = a["browser_download_url"]
                break
        if not url:
            return None
        req = urllib.request.Request(url, headers={"User-Agent": "sub_merge"})
        with urllib.request.urlopen(req, timeout=300) as r:
            raw = gzip.decompress(r.read())
        tmp = MIHOMO_BIN + ".tmp"
        with open(tmp, "wb") as f:
            f.write(raw)
        os.chmod(tmp, 0o755)
        os.replace(tmp, MIHOMO_BIN)
        print("mihomo downloaded -> %s" % MIHOMO_BIN)
        return MIHOMO_BIN if _mihomo_ok(MIHOMO_BIN) else None
    except Exception as e:
        print("  ! mihomo unavailable (%s); real test skipped" % e,
              file=sys.stderr)
        return None


def _chunk_config(items):
    """items: [(临时名, clash dict)]，生成一份最小 mihomo 配置。"""
    lines = ["mixed-port: %d" % MIHOMO_MIXED_PORT,
             "allow-lan: false",
             "mode: rule",
             "log-level: warning",
             "external-controller: 127.0.0.1:%d" % MIHOMO_CTRL_PORT,
             "proxies:"]
    for tname, d in items:
        dd = dict(d)
        dd["name"] = tname
        lines.append("  - " + yaml_flow(dd))
    lines.append("proxy-groups:")
    lines.append("  - {name: TEST, type: select, proxies: [%s]}"
                 % ", ".join(t for t, _ in items))
    lines.append("rules:")
    lines.append("  - MATCH,TEST")
    return "\n".join(lines) + "\n"


def _kill(proc):
    try:
        proc.terminate()
        proc.wait(timeout=5)
    except Exception:
        try:
            proc.kill()
        except Exception:
            pass


def _mihomo_start(binpath, config_text, tmpdir, expect_names=None):
    """启动一个 mihomo 实例，等控制 API 就绪且期望的代理名全部注册完成
    （mihomo 的 API 会先于代理注册上线，只等 /version 会测到 404）；
    失败返回 None。个别名字始终不注册（被内核静默丢弃）时，等注册数
    稳定后照常返回，那几个名字测速时自然 404 判失败。"""
    cfg = os.path.join(tmpdir, "config.yaml")
    with open(cfg, "w", encoding="utf-8") as f:
        f.write(config_text)
    logf = open(os.path.join(tmpdir, "mihomo.log"), "wb")
    try:
        proc = subprocess.Popen([binpath, "-d", tmpdir, "-f", cfg],
                                stdout=logf, stderr=logf)
    except Exception:
        logf.close()
        return None

    def api_ready():
        try:
            req = urllib.request.Request(
                "http://127.0.0.1:%d/version" % MIHOMO_CTRL_PORT)
            with urllib.request.urlopen(req, timeout=2) as r:
                return r.status == 200
        except Exception:
            return False

    def registered():
        try:
            req = urllib.request.Request(
                "http://127.0.0.1:%d/proxies" % MIHOMO_CTRL_PORT)
            with urllib.request.urlopen(req, timeout=2) as r:
                return set(json.load(r).get("proxies", {}))
        except Exception:
            return set()

    deadline = time.time() + 20
    while time.time() < deadline:
        if proc.poll() is not None:
            logf.close()
            return None
        if api_ready():
            break
        time.sleep(0.3)
    else:
        _kill(proc)
        logf.close()
        return None
    if expect_names:
        want = set(expect_names)
        deadline = time.time() + 30
        last_n, last_change = -1, time.time()
        while time.time() < deadline:
            if proc.poll() is not None:
                logf.close()
                return None
            have = registered()
            if want <= have:
                break
            if len(have) != last_n:
                last_n, last_change = len(have), time.time()
            elif time.time() - last_change > 3:
                break
            time.sleep(0.3)
    return proc


def _delay(tname):
    """经指定代理真实访问测试 URL，返回延迟毫秒；不通返回 None。"""
    q = urllib.parse.urlencode({"timeout": REAL_TIMEOUT_MS, "url": TEST_URL})
    req = urllib.request.Request(
        "http://127.0.0.1:%d/proxies/%s/delay?%s"
        % (MIHOMO_CTRL_PORT, urllib.parse.quote(tname), q))
    try:
        with urllib.request.urlopen(
                req, timeout=REAL_TIMEOUT_MS / 1000 + 4) as r:
            d = json.load(r).get("delay")
            return int(d) if d else None
    except Exception:
        return None


def _test_chunk(binpath, items, tmpdir, stats):
    """测一块候选。返回 {临时名: 延迟或 None}。
    整块加载失败说明混有内核拒绝的“毒条目”，对分隔离到单条：
    单条仍加载失败的才计入 real_bad 跳过，其余照常测。"""
    proc = _mihomo_start(binpath, _chunk_config(items), tmpdir,
                         expect_names=[t for t, _ in items])
    if proc is None:
        time.sleep(0.5)
        if len(items) > 1:
            mid = len(items) // 2
            out = _test_chunk(binpath, items[:mid], tmpdir, stats)
            out.update(_test_chunk(binpath, items[mid:], tmpdir, stats))
            return out
        stats["real_bad"] += len(items)
        return {t: None for t, _ in items}
    try:
        stats["real_tested"] += len(items)
        names = [t for t, _ in items]
        with ThreadPoolExecutor(max_workers=REAL_WORKERS) as ex:
            delays = list(ex.map(_delay, names))
        return dict(zip(names, delays))
    finally:
        _kill(proc)


def real_test(nodes, stats):
    """对一级过滤后的节点做真实测速选拔。返回最终选中的节点列表
    （按延迟升序、同一服务器限额后截取双份配额）；mihomo 不可用返回 None。"""
    binpath = ensure_mihomo()
    if not binpath:
        return None
    cand = [n for n in nodes if n.get("d") is not None][:REAL_CAP]
    if not cand:
        return []
    passed = []
    tmpdir = tempfile.mkdtemp(prefix="mihomo-test-")
    deadline = time.time() + REAL_DEADLINE
    try:
        for off in range(0, len(cand), REAL_CHUNK):
            chunk = cand[off:off + REAL_CHUNK]
            items = [("t%d" % (off + j), n["d"])
                     for j, n in enumerate(chunk)]
            res = _test_chunk(binpath, items, tmpdir, stats)
            for (tname, _), n in zip(items, chunk):
                if res.get(tname):
                    n["delay"] = res[tname]
                    passed.append(n)
            stats["real_passed"] = len(passed)
            got_uri = sum(1 for n in passed if n.get("uri"))
            if len(passed) >= MAX_NODES * 5 // 4 \
                    and got_uri >= MAX_NODES * 5 // 4:
                break
            if time.time() > deadline:
                print("  ! real test deadline reached; %d passed so far"
                      % len(passed), file=sys.stderr)
                break
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
    passed.sort(key=lambda n: n["delay"])
    selected, ipcnt, n_clash, n_uri = [], {}, 0, 0
    for n in passed:
        has_d = n.get("d") is not None
        has_u = bool(n.get("uri"))
        if not ((has_d and n_clash < MAX_NODES)
                or (has_u and n_uri < MAX_NODES)):
            continue
        ip = str(n.get("server") or "")
        if ipcnt.get(ip, 0) >= PER_IP_CAP:
            continue
        ipcnt[ip] = ipcnt.get(ip, 0) + 1
        selected.append(n)
        n_clash += 1 if has_d else 0
        n_uri += 1 if has_u else 0
        if n_clash >= MAX_NODES and n_uri >= MAX_NODES:
            break
    return selected


# ---------- 主流程 ----------

def extract_uris_from_text(text):
    uris = set(m.group(0) for m in URI_RE.finditer(text))
    # 整体是 base64 订阅时，明文扫不到，解开再扫
    if len(uris) < 3:
        try:
            dec = b64d(text)
            uris |= set(m.group(0) for m in URI_RE.finditer(dec))
        except Exception:
            pass
    return uris


UDP_PROTOS = ("hysteria", "hysteria2", "hy2", "tuic")


def tcp_alive(host, port, timeout=3.0):
    try:
        with socket.create_connection((host, int(port)), timeout=timeout):
            return True
    except Exception:
        return False


def filter_alive(nodes, stats):
    """按（服务器+端口）端点做 TCP 连通检查，同一端点只测一次；
    UDP 协议（hy2/tuic）和抠不到地址的节点跳过检查、直接保留。"""
    if not CHECK_ALIVE:
        stats["unchecked"] = len(nodes)
        return nodes
    endpoints = {}
    for n in nodes:
        if n["proto"] in UDP_PROTOS or not n["server"] or not n["port"]:
            n["check"] = "skip"
        else:
            endpoints.setdefault((n["server"], n["port"]), None)
    stats["endpoints"] = len(endpoints)
    if endpoints:
        with ThreadPoolExecutor(max_workers=CHECK_WORKERS) as ex:
            for ep, ok in ex.map(
                    lambda ep: (ep, tcp_alive(ep[0], ep[1], CHECK_TIMEOUT)),
                    list(endpoints)):
                endpoints[ep] = ok
    stats["alive_endpoints"] = sum(1 for v in endpoints.values() if v)
    kept = []
    for n in nodes:
        if n.get("check") == "skip":
            stats["unchecked"] += 1
            kept.append(n)
        elif endpoints.get((n["server"], n["port"])):
            kept.append(n)
        else:
            stats["dead_dropped"] += 1
    return kept


def interleave(nodes):
    """按来源轮询交错排序，让裁剪到上限时每个来源都有份。"""
    buckets = {}
    for n in nodes:
        buckets.setdefault(n.get("src", 0), deque()).append(n)
    out = []
    keys = sorted(buckets)
    while any(buckets[k] for k in keys):
        for k in keys:
            if buckets[k]:
                out.append(buckets[k].popleft())
    return out


def process(texts):
    """texts: 各来源文件内容。返回 (v2ray_uris, clash_lines, stats, names)。"""
    seen = set()
    nodes = []
    stats = {"uri_total": 0, "yaml_total": 0, "dup": 0, "ss_dropped": 0,
             "bad": 0, "clash_bad": 0, "endpoints": 0, "alive_endpoints": 0,
             "dead_dropped": 0, "unchecked": 0,
             "real_tested": 0, "real_passed": 0, "real_bad": 0}

    for src_i, text in enumerate(texts):
        # 1) URI 节点
        for uri in extract_uris_from_text(text):
            proto = uri.split("://", 1)[0].lower()
            if proto == "ss":
                stats["ss_dropped"] += 1
                continue
            parser = PARSERS.get(proto)
            parsed = parser(uri) if parser else None
            if not parsed:
                # 解析不了的 URI：v2ray 版按原文保留（整串去重），clash 版跳过；
                # 尽量抠出 host:port 参与存活检查，抠不到就跳过检查
                key = ("raw", uri)
                if key in seen:
                    stats["dup"] += 1
                    continue
                seen.add(key)
                host, port = "", 0
                try:
                    u = urllib.parse.urlsplit(uri)
                    host, port = u.hostname or "", u.port or 0
                except Exception:
                    pass
                nodes.append({"uri": uri, "d": None, "entry": None,
                              "proto": proto, "server": host, "port": port,
                              "name": "", "src": src_i})
                continue
            key, name, d = parsed
            if key in seen:
                stats["dup"] += 1
                continue
            seen.add(key)
            nodes.append({"uri": uri, "d": d, "entry": None,
                          "proto": key[0], "server": key[1], "port": key[2],
                          "name": name, "src": src_i})
        # 2) Clash YAML 来源：条目先解析重组 + 校验，绝不原文透传
        if "proxies:" in text:
            for entry in extract_yaml_proxies(text):
                if not entry:
                    continue
                try:
                    if entry[0].lstrip().startswith("- {"):
                        raw_d = parse_flow_entry(entry)
                    else:
                        raw_d = parse_block_entry(entry)
                except Exception:
                    stats["bad"] += 1
                    continue
                if str(raw_d.get("type") or "").lower() == "ss":
                    stats["ss_dropped"] += 1
                    continue
                d = validate_proxy(convert_yaml_proxy(raw_d))
                if d is None:
                    stats["bad"] += 1
                    continue
                cred = d.get("uuid") or d.get("password") or hashlib.md5(
                    json.dumps(d, sort_keys=True,
                               ensure_ascii=False).encode()
                    ).hexdigest()[:12]
                key = (d["type"], d.get("server"), d.get("port"), cred)
                if key in seen:
                    stats["dup"] += 1
                    continue
                seen.add(key)
                nodes.append({"uri": None, "d": d, "entry": None,
                              "proto": d["type"],
                              "server": d.get("server"),
                              "port": d.get("port"),
                              "name": str(d.get("name") or
                                          "%s-%s-%s" % (d["type"],
                                                        d.get("server"),
                                                        d.get("port"))),
                              "src": src_i})

    ordered = interleave(filter_alive(nodes, stats))

    selected = real_test(ordered, stats) if REAL_TEST else None
    if selected is None:
        selected = ordered

    pool = NamePool()
    v2ray_uris, clash_lines = [], []
    for n in selected:
        if n["uri"] and len(v2ray_uris) < MAX_NODES:
            v2ray_uris.append(n["uri"])
            stats["uri_total"] += 1
        if len(clash_lines) >= MAX_NODES:
            continue
        if n["d"] is not None:
            dv = validate_proxy(n["d"])
            if dv is None:
                stats["clash_bad"] += 1
                continue
            d = dict(dv)
            d["name"] = pool.take(clean_name(n["name"], "node"))
            clash_lines.append("  - " + yaml_flow(d))
            if not n["uri"]:
                stats["yaml_total"] += 1
    return v2ray_uris, clash_lines, stats, pool.order


# ---------- ACL4SSR ini 模板（策略组 + 规则集） ----------

def parse_acl4ssr_ini(text):
    """解析 subconverter 风格 ini 的 custom_proxy_group 与 ruleset 两类行。"""
    groups, rulesets = [], []
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith(";") or line.startswith("#"):
            continue
        if line.startswith("ruleset="):
            target, _, rest = line[len("ruleset="):].partition(",")
            target, rest = target.strip(), rest.strip()
            if rest.startswith("[]"):
                rulesets.append({"target": target,
                                 "special": rest[2:].strip()})
            elif rest:
                rulesets.append({"target": target,
                                 "url": rest.split(",")[0].strip()})
        elif line.startswith("custom_proxy_group="):
            fields = [f.strip() for f in
                      line[len("custom_proxy_group="):].split("`")]
            if len(fields) >= 2:
                groups.append({"name": fields[0], "type": fields[1],
                               "fields": fields[2:]})
    return {"groups": groups, "rulesets": rulesets}


def resolve_group(g, names):
    """把一个策略组定义展开成 (proxies 名单, 附加参数 dict)。"""
    out, extra = [], {}
    for f in g["fields"]:
        if not f:
            continue
        if f.startswith("[]"):
            ref = f[2:]
            if ref and ref not in out:
                out.append(ref)
        elif f.startswith(("http://", "https://")):
            extra["url"] = f
        elif re.fullmatch(r"[\d,]+", f):
            parts = f.split(",")
            if parts[0]:
                extra["interval"] = int(parts[0])
            if len(parts) > 2 and parts[2]:
                extra["tolerance"] = int(parts[2])
        else:
            try:
                pat = re.compile(f)
            except re.error:
                continue
            for n in names:
                if pat.search(n) and n not in out:
                    out.append(n)
    return out, extra


def provider_name(url, used):
    base = url.rsplit("/", 1)[-1]
    base = re.sub(r"\.(list|yaml|yml|txt)$", "", base)
    base = re.sub(r"[^A-Za-z0-9_-]+", "_", base) or "ruleset"
    name, i = base, 2
    while name in used:
        name = "%s_%d" % (base, i)
        i += 1
    used.add(name)
    return name


def build_clash(clash_lines, names, template=None):
    head = [
        "# merged by sub_merge.py at %s" % datetime.now().strftime(
            "%Y-%m-%d %H:%M"),
        "mixed-port: 7890",
        "allow-lan: false",
        "mode: rule",
        "log-level: info",
        "proxies:",
    ]
    body = list(clash_lines)
    if not template or not template.get("groups"):
        # 回退：单个选择组 + MATCH
        group = ["proxy-groups:",
                 "  - name: %s" % json.dumps(GROUP_NAME, ensure_ascii=False),
                 "    type: select",
                 "    proxies:"]
        group += ["      - %s" % json.dumps(n, ensure_ascii=False)
                  for n in names]
        group += ["      - \"DIRECT\"", "rules:",
                  "  - %s" % json.dumps("MATCH," + GROUP_NAME,
                                        ensure_ascii=False)]
        return "\n".join(head + body + group) + "\n"

    # 策略组（按 ini 定义）
    body.append("proxy-groups:")
    for g in template["groups"]:
        proxies, extra = resolve_group(g, names)
        if not proxies:
            proxies = ["DIRECT"]
        body.append("  - name: %s" % json.dumps(g["name"],
                                                ensure_ascii=False))
        body.append("    type: %s" % g["type"])
        if "url" in extra:
            body.append("    url: %s" % extra["url"])
        if "interval" in extra:
            body.append("    interval: %d" % extra["interval"])
        if "tolerance" in extra:
            body.append("    tolerance: %d" % extra["tolerance"])
        body.append("    proxies:")
        body += ["      - %s" % json.dumps(p, ensure_ascii=False)
                 for p in proxies]
    # 规则集提供者 + 规则（按 ini 顺序）
    used, providers, rules = set(), [], []
    for rs in template["rulesets"]:
        if "url" in rs:
            pname = provider_name(rs["url"], used)
            providers.append((pname, rs["url"]))
            rules.append("RULE-SET,%s,%s" % (pname, rs["target"]))
        elif rs.get("special") == "FINAL":
            rules.append("MATCH,%s" % rs["target"])
        elif rs.get("special"):
            rules.append("%s,%s" % (rs["special"], rs["target"]))
    if providers:
        body.append("rule-providers:")
        for pname, url in providers:
            body.append("  %s:" % pname)
            body.append("    type: http")
            body.append("    behavior: classical")
            body.append("    url: %s" % json.dumps(url, ensure_ascii=False))
            body.append("    path: ./ruleset/%s.list" % pname)
            body.append("    interval: 86400")
    body.append("rules:")
    body += ["  - %s" % json.dumps(r, ensure_ascii=False) for r in rules]
    return "\n".join(head + body) + "\n"


def main():
    if not os.path.exists(LINKS_FILE):
        print("links file not found: %s (run sub_collector.py first)"
              % LINKS_FILE, file=sys.stderr)
        return 1
    links = [ln.strip() for ln in open(LINKS_FILE, encoding="utf-8")
             if ln.strip().startswith("http")]
    print("Merging %d source subscriptions ..." % len(links))
    texts = []
    for i, url in enumerate(links, 1):
        try:
            texts.append(http_get(url).decode("utf-8", "ignore"))
        except Exception as e:
            print("  ! [%d/%d] fetch failed %s: %s" % (i, len(links), url, e),
                  file=sys.stderr)
    v2ray_uris, clash_lines, stats, names = process(texts)

    template = None
    try:
        ini_text = http_get(INI_URL, timeout=15).decode("utf-8", "ignore")
        parsed = parse_acl4ssr_ini(ini_text)
        if parsed["groups"]:
            template = parsed
            print("ACL4SSR template loaded: %d groups, %d rulesets"
                  % (len(parsed["groups"]), len(parsed["rulesets"])))
    except Exception as e:
        print("  ! ACL4SSR ini fetch failed (%s); fallback groups" % e,
              file=sys.stderr)

    os.makedirs(OUT_DIR, exist_ok=True)
    plain = "\n".join(v2ray_uris) + ("\n" if v2ray_uris else "")
    with open(os.path.join(OUT_DIR, "merged-v2ray-plain.txt"), "w",
              encoding="utf-8") as f:
        f.write(plain)
    with open(os.path.join(OUT_DIR, "merged-v2ray.txt"), "w",
              encoding="utf-8") as f:
        f.write(base64.b64encode(plain.encode()).decode() + "\n")
    with open(os.path.join(OUT_DIR, "merged-clash.yaml"), "w",
              encoding="utf-8") as f:
        f.write(build_clash(clash_lines, names, template))
    summary = ("sources=%d v2ray_nodes=%d clash_proxies=%d dup_removed=%d "
               "ss_dropped=%d bad_skipped=%d clash_bad=%d endpoints=%d "
               "alive_endpoints=%d dead_dropped=%d unchecked_udp=%d "
               "real_tested=%d real_passed=%d real_bad=%d" % (
                   len(links), len(v2ray_uris), len(clash_lines),
                   stats["dup"], stats["ss_dropped"], stats["bad"],
                   stats["clash_bad"], stats["endpoints"],
                   stats["alive_endpoints"], stats["dead_dropped"],
                   stats["unchecked"], stats["real_tested"],
                   stats["real_passed"], stats["real_bad"]))
    with open(os.path.join(OUT_DIR, "merge-stats.txt"), "w",
              encoding="utf-8") as f:
        f.write(summary + "\n")
    print("Done: " + summary)
    return 0


if __name__ == "__main__":
    sys.exit(main())
