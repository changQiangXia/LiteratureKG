# LiteratureKG (唐宋文学知识图谱 MVP)

本项目面向汉语言文学研究场景，提供从唐宋诗歌语料到 Neo4j 图谱的完整流程：  
数据构建 -> 关系抽取 -> 历史地名规范化 -> 证据链审计 -> AuraDB 导入 -> 查询与评估。

## 1. 核心特性

- 唐宋诗歌抽样与结构化（诗人、诗作、时代、地名、意象）
- 地点清洗白名单与噪声过滤
- 简体中文统一输出（OpenCC）
- 历史地名规范化层：`Place -> PlaceCanonical -> City`
- 可审计证据链：`Poem -> Evidence -> Place/Image`
- 自动标注与评估脚本（地点、意象）
- 高风险优先人工复核清单（Top50 + Top50）

## 2. 图谱模式

### 2.1 节点

- `Era`
- `Poet`
- `Poem`
- `City`
- `Place`
- `PlaceCanonical`
- `Evidence`
- `Image`
- `NarrativeType`
- `DiscourseConcept`
- `Paper`

### 2.2 关系

- `WROTE`
- `CREATED_IN`
- `MENTIONS_PLACE`
- `USES_IMAGE`
- `HAS_NARRATIVE`
- `EMBODIES_DISCOURSE`
- `DISCUSSED_IN`
- `LOCATED_IN`
- `NORMALIZED_TO`
- `CANON_LOCATED_IN`
- `HAS_EVIDENCE`
- `SUPPORTS_PLACE`
- `SUPPORTS_IMAGE`

## 3. 项目结构

```text
LiteratureKG/
├─ scripts/
│  ├─ build_mvp_dataset.py
│  ├─ import_csv_to_auradb.py
│  ├─ test_auradb_connection.py
│  ├─ annotate_place_gold_v1.py
│  ├─ annotate_image_gold_v1.py
│  ├─ eval_place_precision_from_gold.py
│  ├─ eval_image_precision_from_gold.py
│  └─ build_manual_review_checklist_v1.py
├─ data/
│  ├─ input/        # 运行后生成或手动下载的数据 CSV（默认不上传）
│  ├─ annotation/   # 标注结果（可选）
│  └─ cache/        # 本地缓存（默认不上传）
├─ cypher/
│  └─ demo_queries.cql
├─ .env.example
├─ requirements.txt
├─ QUICKSTART_AURADB.md
└─ IMPORT_AURADB.md
```

## 4. 环境准备

### 4.1 创建 Python 环境

推荐 Python `3.11` + Conda：

```powershell
conda create -n litkg python=3.11 -y
conda activate litkg
python -m pip install -r requirements.txt
```

### 4.2 配置 `.env`（必须）

先复制模板：

```powershell
Copy-Item .env.example .env
```

再编辑 `.env`，填写 AuraDB 连接信息：

```ini
NEO4J_URI=neo4j+s://xxxx.databases.neo4j.io
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=your_password
NEO4J_DATABASE=neo4j
```

字段说明：

- `NEO4J_URI`：Aura 实例连接地址，必须是 `neo4j+s://` 开头
- `NEO4J_USERNAME`：数据库用户名，Aura 默认通常是 `neo4j`
- `NEO4J_PASSWORD`：Aura 实例密码（创建实例时设置，或在 Aura 控制台重置）
- `NEO4J_DATABASE`：数据库名，Aura Free 通常为 `neo4j`

验证连接：

```powershell
python scripts/test_auradb_connection.py
```

若连接成功，输出中会出现 `Connection OK`。  
`.env` 含敏感信息，不应上传到 GitHub（本仓库已在 `.gitignore` 中忽略）。

## 5. 数据获取（GitHub 仓库默认不带数据集）

为减少仓库体积，本仓库默认不上传大数据文件和缓存文件。  
运行导入脚本前，需要先准备 `data/input` 所需 CSV。

上游数据仓库（官方）：

- `https://github.com/chinese-poetry/chinese-poetry.git`

### 5.1 方式 A：百度网盘下载（占位）

- 百度网盘链接：`xxx`
- 提取码：`xxx`

下载并解压后，按压缩包内说明放置文件，目标是得到 `data/input/*.csv`。

### 5.2 方式 B：本地自动构建（推荐）

```powershell
python scripts/build_mvp_dataset.py --source zip --target-poets 50 --tang-poems 700 --song-poems 800 --max-files-per-dynasty 8
```

说明：

- 若 `data/cache/chinese-poetry/chinese-poetry-master.zip` 不存在，脚本会自动下载
- 若网络受限，可先手动下载 zip 放入 `data/cache/chinese-poetry/`

### 5.3 方式 C：本地原始 JSON 构建

先克隆官方仓库（示例）：

```powershell
git clone https://github.com/chinese-poetry/chinese-poetry.git data/source/chinese-poetry
```

再执行构建：

```powershell
python scripts/build_mvp_dataset.py --source local --local-dir data/source/chinese-poetry
```

`data/source/chinese-poetry` 下应包含 `poet.tang.*.json` 与 `poet.song.*.json`。

## 6. 导入 AuraDB

```powershell
python scripts/import_csv_to_auradb.py --data-dir data/input --batch-size 500
```

导入后可验证：

```cypher
MATCH (n) RETURN labels(n)[0] AS label, count(*) AS cnt ORDER BY cnt DESC;
MATCH ()-[r]->() RETURN type(r) AS rel, count(*) AS cnt ORDER BY cnt DESC;
```

## 7. 标注与评估（可选）

```powershell
python scripts/annotate_place_gold_v1.py
python scripts/eval_place_precision_from_gold.py
python scripts/annotate_image_gold_v1.py
python scripts/eval_image_precision_from_gold.py
python scripts/build_manual_review_checklist_v1.py
```

## 8. 查询示例

更多查询见 `cypher/demo_queries.cql`。

```cypher
MATCH p=(poet:Poet)-[:WROTE]->(poem:Poem)-[:USES_IMAGE]->(img:Image)
RETURN p
LIMIT 100;
```

```cypher
MATCH p=(poem:Poem)-[:MENTIONS_PLACE]->(pl:Place)-[:NORMALIZED_TO]->(pc:PlaceCanonical)-[:CANON_LOCATED_IN]->(city:City)
RETURN p
LIMIT 100;
```

## 9. Bloom 可视化说明（重要）

在 Neo4j Bloom 中，可能出现“画布上有很多点看起来没有线”的情况。  
这不一定代表数据库有孤立节点，常见原因：

- 当前画布仅显示了部分关系（过滤器或展开上限）
- 导出 SVG 时保留了部分节点，但关系线被裁剪
- 使用了 `RETURN n`（只返回节点）而非 `RETURN p`（返回路径）

已在 `2026-03-02` 核验：

- 全图孤立节点为 `0`
- 某次 Bloom 导出 SVG 含 `116` 个节点，仅绘制 `84` 条线
- 同一批节点在数据库中实际关系数为 `461`

建议复试演示使用 `MATCH p=... RETURN p`。

## 10. 常见问题

- `Unable to retrieve routing information`  
  检查 `NEO4J_URI` 是否为 `neo4j+s://...`，并确认网络可访问 Aura。
- `Authentication failed`  
  检查账号密码，必要时在 Aura 控制台重置密码。
- PowerShell 中文乱码  
  通常是终端显示编码问题，CSV 实际为 UTF-8-SIG，Python 读写一般正常。

## 11. GitHub 发布说明（请务必看）

### 11.1 本仓库默认不上传的文件

以下文件已在 `.gitignore` 中显式忽略：

- `teachers.jpg`
- `chinese-poetry-master.zip`
- `data/cache/chinese-poetry/chinese-poetry-master.zip`
- `data/cache/` 下其他缓存
- `.env`（凭据）

### 11.2 建议上传的内容

- `scripts/`
- `cypher/demo_queries.cql`
- `requirements.txt`
- `.env.example`
- `README.md`、`QUICKSTART_AURADB.md`、`IMPORT_AURADB.md`

### 11.3 如果项目选择“不上传任何数据集”

这是允许的，但 README 需要保留“数据获取”说明（百度网盘 `xxx` 或本地自动构建）。  
这样 GitHub 用户下载后仍可复现。

### 11.4 Push 前检查（建议）

```powershell
git status
git check-ignore -v teachers.jpg chinese-poetry-master.zip data/cache/chinese-poetry/chinese-poetry-master.zip
```

如果这些文件之前已经被加入过 Git 暂存区，需要先取消跟踪：

```powershell
git rm --cached teachers.jpg chinese-poetry-master.zip data/cache/chinese-poetry/chinese-poetry-master.zip
```

## 12. 数据来源与边界

- 诗歌语料来源：`https://github.com/chinese-poetry/chinese-poetry.git`
- 本项目用于学术实验与方法演示，建议在论文/汇报中说明“自动抽取 + 人工复核”的边界。
