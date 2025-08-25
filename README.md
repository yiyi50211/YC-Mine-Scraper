# YCombinator 职位爬虫项目

一个自动化的YCombinator公司职位信息爬取和管理系统，支持批量爬取、数据合并和Supabase数据库同步。

## 🚀 项目概述

本项目能够：
- 自动获取YCombinator所有公司信息
- 批量爬取各公司的职位信息
- 将职位数据与公司信息进行关联合并
- 自动同步数据到Supabase数据库
- 提供完整的日志记录和错误处理

## 📁 项目结构

```
new_scraper/
├── README.md                              # 项目说明文档
├── .env                                   # 环境变量配置
├── requirements.txt                       # Python依赖包
├── 
├── 核心脚本/
│   ├── run_complete_scraper.py           # 主控制脚本（一键运行）
│   ├── fetch_yc_companies.py             # 获取YC公司数据
│   ├── batch_scraper.py                  # 批量职位爬取
│   ├── job_scraper_improved.py           # 单公司职位爬取
│   └── update_jobs_with_company_info_merged.py  # 数据合并
│
├── 数据库同步/
│   ├── update_supabase_tables.py         # 数据库更新脚本
│   └── test_supabase_connection.py       # 数据库连接测试
│
├── 输出目录/
│   ├── result/                           # 所有输出文件
│   │   ├── all_hiring_companies_*.csv    # YC公司数据
│   │   ├── all_jobs_*.csv               # 职位数据
│   │   ├── all_jobs_company_info_*.csv  # 合并后数据
│   │   └── *.json                       # JSON格式数据
│   └── logs/                            # 日志文件
│
└── 历史文件/
    └── upload_to_supabase_simple.py     # 旧版上传脚本
```

## 🛠️ 环境配置

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置环境变量

创建 `.env` 文件并添加以下配置：

```env
# YC登录凭据
LOGIN_USERNAME=your_email@example.com
LOGIN_PASSWORD=your_password

# Supabase配置
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_ANON_KEY=your_anon_key
```

### 3. 创建必要目录

```bash
mkdir -p result logs
```

## 🎯 使用方法

### 方式一：一键运行（推荐）

```bash
python run_complete_scraper.py
```

这将自动执行完整的三步流程：
1. 获取YC公司数据
2. 批量爬取职位信息
3. 合并数据并生成最终文件

### 方式二：分步执行

#### 步骤1：获取YC公司数据
```bash
python fetch_yc_companies.py
```

#### 步骤2：批量爬取职位
```bash
python batch_scraper.py
```

#### 步骤3：合并数据
```bash
python update_jobs_with_company_info_merged.py
```

### 方式三：数据库同步

#### 测试数据库连接
```bash
python test_supabase_connection.py
```

#### 同步数据到Supabase
```bash
python update_supabase_tables.py
```

## 📊 数据说明

### 输出文件格式

所有文件都带有时间戳，格式为：`文件名_YYYYMMDD_HHMMSS.csv`

#### 1. YC公司数据 (`all_hiring_companies_*.csv`)
包含25个字段：
- 基本信息：`name`, `slug`, `website`, `one_liner`
- 详细信息：`long_description`, `team_size`, `founded_year`
- 位置信息：`all_locations`, `hq_location`
- 融资信息：`total_funding`, `latest_funding_stage`
- 其他：`logo_url`, `batch`, `tags` 等

#### 2. 职位数据 (`all_jobs_*.csv`)
包含17个字段：
- 职位信息：`job_name`, `company`, `link_url`, `apply_url`
- 薪资信息：`salaryRange`, `equityRange`
- 要求：`minExperience`, `minSchoolYear`, `visa`
- 其他：`job_id`, `location`, `remote`, `fullTime` 等

#### 3. 合并数据 (`all_jobs_company_info_*.csv`)
包含43个字段（职位字段 + 公司字段的完整合并）

### Supabase数据库表

- **hiring_companies**: 存储YC公司信息（1275+条记录）
- **yc_jobs**: 存储职位信息（3589+条记录）
- **yc_jobs_join**: 存储合并后的完整数据（3601+条记录）

## ⚙️ 核心功能

### 1. 智能文件管理
- 自动查找最新文件（优先result目录，兼容根目录）
- 带时间戳的文件命名避免覆盖
- 支持CSV和JSON双格式输出

### 2. 数据处理优化
- `minExperience`字段标准化（"Any (new grads ok)" → "0"）
- 自动处理编码问题（UTF-8/GBK兼容）
- 空值填充和数据清洗

### 3. 批量处理
- 支持大规模数据爬取（1000+公司）
- 智能重试机制和错误恢复
- 详细的进度跟踪和日志记录

### 4. 数据库同步
- 批量插入优化（每批100条记录）
- 自动表清理和数据更新
- 完整的错误处理和回滚机制

## 📈 性能指标

- **公司数量**: 1275+ YC公司
- **职位数量**: 3500+ 活跃职位
- **爬取速度**: ~5小时完成全量爬取
- **成功率**: >95% 数据获取成功率
- **数据完整性**: 43个字段的完整职位+公司信息

## 🔧 故障排除

### 常见问题

1. **登录失败**
   - 检查`.env`文件中的用户名密码
   - 确认YC账户状态正常

2. **爬取中断**
   - 查看`logs/`目录中的日志文件
   - 重新运行脚本会自动跳过已完成的公司

3. **数据库连接失败**
   - 运行`python test_supabase_connection.py`测试连接
   - 检查Supabase配置和网络连接

4. **文件找不到**
   - 确认文件在`result/`目录中
   - 检查文件名时间戳格式

### 日志文件

所有操作都会生成详细日志：
- `logs/batch_scraper_*.log` - 批量爬取日志
- `logs/supabase_update_*.log` - 数据库更新日志
- `logs/complete_scraper_*.log` - 完整流程日志

## 🔄 更新流程

### 定期更新数据

1. 运行完整爬取流程：
```bash
python run_complete_scraper.py
```

2. 同步到数据库：
```bash
python update_supabase_tables.py
```

### 增量更新

对于单个公司的更新：
```bash
python job_scraper_improved.py
```

## 📝 开发说明

### 代码规范
- 使用UTF-8编码
- 遵循PEP 8代码风格
- 完整的错误处理和日志记录
- 模块化设计，职责分离

### 扩展功能
- 支持添加新的数据字段
- 可配置的爬取参数
- 灵活的数据输出格式
- 可扩展的数据库支持

## 📄 许可证

本项目仅供学习和研究使用，请遵守相关网站的使用条款。

## 🤝 贡献

欢迎提交Issue和Pull Request来改进项目。

---

**最后更新**: 2025-08-24
**版本**: v2.0
**维护者**: YC Job Scraper Team