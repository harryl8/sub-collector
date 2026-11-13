# sub-collector

从 GitHub 自动采集免费 V2Ray / Clash 订阅、逐个节点**真实测速**后合并出货的两件套工具。

免费订阅最大的问题不是找不到，而是找到的节点大多已经死了：端口还开着、协议早已失效，普通"存活检查"根本筛不出来。这套工具的合并环节会把候选节点全量加载进 [mihomo](https://github.com/MetaCubeX/mihomo) 内核，逐个真的经节点访问测试页，**只有拿到响应才算通过**，发布出去的每个节点都是发布前几分钟刚实测可用的。

纯 Python 3 标准库实现，零 pip 依赖；mihomo 二进制缺失时脚本会自动从 GitHub 下载对应架构的最新版。

## 工作流程

```
sub_collector.py                     sub_merge.py
─────────────────                    ─────────────────────────────
GitHub API 搜索近 7 天更新的     →   抓取全部订阅 → 解析节点（URI / base64 /
订阅仓库 → 挑选 all/综合订阅          Clash YAML 三路）→ 按（协议+服务器+
文件 → 轻量校验 →                    端口+凭证）去重 → 剔除 SS → TCP 粗筛
out/all_links.txt                    → mihomo 真实测速 → 按延迟排序 →
                                     同服务器限额 → out/merged-*
```

## 快速开始

```bash
git clone https://github.com/harryl8/sub-collector.git
cd sub-collector

python3 sub_collector.py   # 采集，生成 out/all_links.txt
python3 sub_merge.py       # 合并+测速，生成 out/merged-v2ray.txt、out/merged-clash.yaml
```

对外发布（任意静态 HTTP 服务都可以）：

```bash
cd out && python3 -m http.server 8351
# 订阅地址示例：
#   http://<你的IP>:8351/merged-clash.yaml
#   http://<你的IP>:8351/merged-v2ray.txt
```

## 输出文件

| 文件 | 说明 |
|---|---|
| `out/latest.txt` | 采集清单：仓库 / 更新时间 / raw 链接 / 约节点数 |
| `out/all_links.txt` | 纯订阅链接列表（合并器的输入） |
| `out/report-YYYY-MM-DD.md` | 采集日报 |
| `out/merged-v2ray.txt` | 合并订阅（base64 通用格式，v2rayN 等客户端可用） |
| `out/merged-v2ray-plain.txt` | 同上明文版（排查用） |
| `out/merged-clash.yaml` | Clash / mihomo 完整配置（节点 + ACL4SSR 策略组与规则） |
| `out/merge-stats.txt` | 本次合并统计，即日志里的 Done 行 |

## 定时运行

每 8 小时跑一轮（采集和合并链式执行，`sub_merge` 必须排在采集之后）：

```cron
CRON_TZ=Asia/Shanghai
20 */8 * * * /usr/bin/python3 /path/to/sub_collector.py >> /path/to/run.log 2>&1; /usr/bin/python3 /path/to/sub_merge.py >> /path/to/merge.log 2>&1
```

## 环境变量

**sub_collector.py**

| 变量 | 默认 | 说明 |
|---|---|---|
| `GITHUB_TOKEN` | 空 | 填了可把 GitHub API 限额从 60 次/小时提到 5000 次/小时（不填也能跑） |
| `MAX_REPOS` | 60 | 最多深入扫描的仓库数 |
| `DAYS` | 7 | 只看最近多少天内有更新的仓库 |
| `MAX_LINKS` | 120 | 清单最多输出多少条链接 |

**sub_merge.py**

| 变量 | 默认 | 说明 |
|---|---|---|
| `LINKS_FILE` | `out/all_links.txt` | 订阅链接清单路径 |
| `OUT_DIR` | 与 LINKS_FILE 同目录 | 输出目录 |
| `MAX_NODES` | 200 | 每份输出的节点上限 |
| `ACL4SSR_INI` | ACL4SSR 官方 Mini 版 | Clash 规则 ini 地址，可指向自己的 fork |
| `CHECK_ALIVE` | 1 | 一级 TCP 粗筛开关（0 关） |
| `CHECK_TIMEOUT` / `CHECK_WORKERS` | 3 秒 / 100 | 粗筛超时与并发 |
| `REAL_TEST` | 1 | 二级真实测速开关（0 关，回退到只用一级检查） |
| `REAL_CAP` | 4000 | 真实测速最多测多少个候选节点 |
| `REAL_TIMEOUT_MS` | 6000 | 单节点测速超时（毫秒） |
| `REAL_DEADLINE` | 900 | 真实测速总时长上限（秒），超时用已通过的结果出货 |
| `PER_IP_CAP` | 3 | 最终输出里同一服务器最多留几个节点 |
| `MIHOMO_BIN` | 脚本同目录 `mihomo` | mihomo 二进制路径 |

## 合并统计（Done 行）字段

```
Done: sources=88 v2ray_nodes=187 clash_proxies=200 dup_removed=51018
      ss_dropped=7506 bad_skipped=77 clash_bad=0 endpoints=13505
      alive_endpoints=8114 dead_dropped=6001 unchecked_udp=1100
      real_tested=3958 real_passed=480 real_bad=2
```

- `dup_removed` 按（协议+服务器+端口+凭证）判重删掉的重复节点 —— 同一节点换个备注名也只留一份
- `bad_skipped` / `clash_bad` 字段不合法被剔除的条目（端口非法、UUID 格式错误、占位符服务器、上游混入的示例模板等）
- `real_tested / real_passed / real_bad` 真实测速的已测 / 通过 / 被内核拒绝的毒条目数

## 设计要点

- **两级筛选**：TCP 粗筛先砍掉整批死端点（同一端点只测一次）；真实测速再逐个验证协议层可用性，hysteria2 / tuic 等 UDP 协议同样实测，不再靠"端口活着"蒙混
- **YAML 来源绝不原文透传**：Clash YAML 条目先解析成字段、按类型白名单重组、过合法性校验才输出 —— 免费仓库里混着的示例模板、坏缩进、占位符条目会在这一层被拦下（一条坏条目就能让整份配置导入失败）
- **毒条目隔离**：个别字段缺失的条目会让 mihomo 拒绝加载整份配置，测速时用对分法把坏条目隔离到单条剔除，不连累同批好节点
- **来源公平**：候选按跨来源轮询交错排序，排在前面的订阅不会吃光名额
- **ACL4SSR 规则**：运行时拉取 ini 模板生成策略组与 rule-providers，拉取失败自动回退单个选择组 + MATCH，不中断出货
- **口径**：SS（ss:// 与 Clash `type: ss`）一律剔除；SSR 不在剔除范围

## 说明

- 未填 `GITHUB_TOKEN` 时 GitHub 核心 API 限额 60 次/小时，采集器扫到限额会自动停扫、用已拿到的结果出清单，不会整轮报废；脚本带 `state.json` 缓存，未更新的仓库直接复用上次结果省额度
- 免费节点时效很短：本工具保证"发布时实测可用"，几小时后仍会自然死亡一批，这是货源性质决定的，勤刷新即可
- 仅供学习与技术研究使用，请遵守当地法律法规

## License

MIT
