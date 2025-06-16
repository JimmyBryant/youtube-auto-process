#!/bin/bash

# ========================
# YouTube视频处理项目生成器
# 精简版（仅脚手架）
# ========================

# 基础配置
PROJECT_NAME="youtube-auto-process"

echo "🚀 开始创建项目 $PROJECT_NAME..."

# ========================
# 1. 创建项目目录结构
# ========================
mkdir -p ${PROJECT_NAME}/{config,data/{downloads,subtitles,comments,outputs},logs,src/{core,modules,utils},scripts,requirements}

# 创建主要Python文件
touch ${PROJECT_NAME}/src/{__init__.py,main.py}
touch ${PROJECT_NAME}/src/core/{__init__.py,task_manager.py,models.py,exceptions.py}
touch ${PROJECT_NAME}/src/modules/{__init__.py,downloader.py,transcriber.py,translator.py,scraper.py,publisher.py}
touch ${PROJECT_NAME}/src/utils/{__init__.py,file_utils.py,log_utils.py,anti_detect.py}

# ========================
# 2. 生成基础配置文件
# ========================
cat > ${PROJECT_NAME}/config/dev.env <<'EOL'
# MongoDB配置
MONGODB_URI="mongodb://localhost:27017"
DB_NAME="youtube_processor"

# 下载设置
DOWNLOAD_DIR="./data/downloads"

# OpenAI配置
OPENAI_API_KEY="your-api-key-here"

# 浏览器设置
HEADLESS_MODE="True"
USER_AGENT="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
EOL

cp ${PROJECT_NAME}/config/dev.env ${PROJECT_NAME}/config/prod.env

# ========================
# 3. 生成requirements.txt
# ========================
cat > ${PROJECT_NAME}/requirements.txt <<'EOL'
# 核心依赖
python-dotenv==1.0.0
pymongo==4.5.0

# 视频处理
yt-dlp==2023.11.16
ffmpeg-python==0.2.0

# 语音处理
openai-whisper==20231117
torch==2.1.1

# 浏览器自动化
playwright==1.39.0

# 翻译服务
openai==1.3.6
langdetect==1.0.9

# 开发工具（可选）
pytest==7.4.3
ipdb==0.13.13
EOL

# ========================
# 4. 生成基础README
# ========================
cat > ${PROJECT_NAME}/README.md <<'EOL'
# YouTube视频自动化处理

## 项目结构
