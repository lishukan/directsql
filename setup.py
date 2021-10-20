from setuptools import setup, find_packages  
  
setup(  
    name = 'directsql',  
    version='0.2.9',
    # keywords = ('chinesename',),  
    description="""一个简单的使用python操作mysql的工具，提供了一些类似sql语法的方法，最终拼接成sql。可以很好地处理一些常见场景，不依赖orm 的同时避免手写大量sql。""",  
    license = 'MIT License',  
    packages = ['directsql'],  # 要打包的项目文件夹
    include_package_data=True,   # 自动打包文件夹内所有数据
    author = 'surecanlee',  
    author_email = 'lishukan@qq.com',
    url = 'https://github.com/lishukan/directsql',
    # packages = find_packages(include=("*"),),  
    install_requires=[
        'DBUtils>=1.13.0',
    ]
)
"""
先把__init__.py 中的版本号和setup.py 中的版本号改下

然后
python3 setup.py bdist_wheel --universal    #编译 纯python的wheel包
python3 setup.py sdist   #编译 源码压缩包
任选其一 

之后 使用  twine upload dist/* 上传 ，用户名是lishukan
dist/ 文件夹下只保留一个文件
"""

