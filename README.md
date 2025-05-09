# Crypto Arbitrage Bot

## Env Setup
本项目使用uv来管理python环境。本项目默认系统环境为MacOS/Linux。
运行前需要：

1. 安装[uv](https://github.com/astral-sh/uv)
2. `uv venv --python=3.13`
3. `source .venv/bin/activate`
4. 在环境变量里设置`TELEGRAM_BOT_TOKEN` `TELEGRAM_CHAT_ID`
5. `python3 main.py`
6. 开始后，程序会持续运行，可以使用`Ctrl + C`来中断程序。

## 项目整体逻辑
技术层面看，本项目需要持续监控DEX与CEX之间的价格差距，并且在套利机会出现时尽快完成交易，而这所有的操作都极度依赖网络IO速度。同时，我们不希望不同任务互相阻塞，因此本项目使用了python的async环境来提高并发效率。
具体而言，本项目中存在五个并行的async任务：

1. 监听DEX，CEX价格。
2. 计算价格差距，判断是否有套利可能。
3. 调用DEX/CEX API，执行套利操作。
4. 将套利结果保存至json。
5. 讲套利结果发送至telegram bot。

不同async任务之间通过Queue交流，以此简化了代码的实现逻辑。

## 项目配置
本项目支持任意交易对的套利操作，只需正确填写`config.yaml`。
