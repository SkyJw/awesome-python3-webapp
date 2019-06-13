import orm
import asyncio
from models import User, Blog, Comment

#因为代码全部采用了asyncio标准异步库，故廖大网站上的yield from方式已经不可用，asyncio协程标准调用方式如下代码所示
#定义主协程函数，调用子协程函数时，加上await，调用主协程函数时，应使用asyncio.get_enent_loop()方法，先创建一个协程循环
#然后使用run_until_complete()等方法运行
async def test(loop):
    await orm.create_pool(loop=loop, user='SkyJw', password='123ll520', db='awesome')
    u = User(name='Test', email='test@qq.com', passwd='1234567890', image='about:blank')
    await u.save()
    ## 网友指出添加到数据库后需要关闭连接池，否则会报错 RuntimeError: Event loop is closed
    orm.__pool.close()
    await orm.__pool.wait_closed()
if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test(loop))
    loop.close()