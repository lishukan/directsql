from setuptools import setup, find_packages  
  
setup(  
    name = 'directsql',  
    version = '0.2.0',
    # keywords = ('chinesename',),  
    description = '文档第一版',  
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
python3 setup.py bdist_wheel --universal    #编译 纯python的wheel包
python3 setup.py sdist   #编译 源码压缩包
任选其一 

之后 使用  twine upload dist/* 上传 ，用户名是lishukan
dist/ 文件夹下只保留一个文件
"""

