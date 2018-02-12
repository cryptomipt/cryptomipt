# Environment
This package is designed to interact with the [data server](https://github.com/cryptomipt/collector) test strategies an historical data, easily deploy strategies to production. By default our data server is used but you can deploy your own data collector and specify the address where needed. It includes exchange emulator, backtest and template trader class.

Pull Requests welcome and encouraged.

Examples are available in examples folder.

# Installation

Get the latest release and run:
```python
python setup.py install
```

# Running example

The backtest class requires exchange connector which can be a real exchange through [ccxt](https://github.com/ccxt/ccxt) or a ccxt-identical exchange emulator.

To get dataset you need to specify date range in exchange emulator constructor. If there is first time with that date range dataset will be downloaded.

![downloading dataset](media/downloading.png)

# Donation
If you want to support us you can make donations in
- BTC [1LFqtqoK8ooVkP4Rt83sjuw4rZZHekVT5L](https://blockchain.info/en/address/1LFqtqoK8ooVkP4Rt83sjuw4rZZHekVT5L)
