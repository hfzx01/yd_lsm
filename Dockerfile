# 使用官方的 Python 镜像作为基础镜像
FROM python:3.8.0

# 设置工作目录
WORKDIR ./app

# 复制项目文件到工作目录
COPY . .

# 安装项目依赖
RUN pip install -r requirements.txt

# 设置环境变量，指定配置文件名，默认为config.ini
ENV CONFIG_FILE_NAME config.ini

# 设置启动命令
CMD ["sh", "-c", "python ./app/consumer.py $CONFIG_FILE_NAME"]
