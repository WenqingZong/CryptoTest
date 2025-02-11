# CryptoTest
Crypto take home test

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

## 项目不足
尽管翻阅了大量的API文档，但十分遗憾在Jupiter链上的操作始终无法完成，具体表现为bot获取到quote和swap信息之后，无法对交易进行签名，进而导致交易无法在solana链上广播出去，不被他人认可。迫于无奈，最终选择了只进行交易模拟而不发生真实的交易数据。