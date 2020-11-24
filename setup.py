from setuptools import setup, find_packages  
  
setup(  
    name = 'directsql',  
    version = '0.0.1',
    # keywords = ('chinesename',),  
    description = 'hello world ,hello me',  
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

