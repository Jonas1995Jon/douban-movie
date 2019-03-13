from scrapy import cmdline

cmdline.execute('scrapy crawl douban_spider'.split())

# 生成CSV文件
# cmdline.execute("scrapy crawl douban_spider -o douban.csv -t csv".split())