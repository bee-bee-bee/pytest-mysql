## 简介
pytest-mysql插件
## 安装

如果在虚拟环境中安装使用pipenv安装：

`pipenv install pytest-mysql -i http://*:8082/private_repository/ --trusted-host *`

## 使用
### 测试用例可使用msyql fixtrue

```python
def test_mysql(mysql):
    data = mysql['rvs'].fetch_one('vehicle_profile', {"id": '74361e94a61846e2a690d2e2a9bf591d'})
```
### 运行测试
需编写pytest.ini文件，置于项目内的根目录上，用于指定mysql配置路径。
默认在项目内的根目录下寻找环境对应配置(./config/config.yml)

####pytest.ini
```ini
[mysql]
config = config/config.yml
```
或在命令行中通过--config_mysql参数指定路径
```bash
pytest --config_mysql config/config.yml
```
####test_config.yml配置如下:
```yaml
mysql:
  rvs:
    host:  your_db_hostname
    port: 3306
    user:  you_user_name
    password:  *
    database:  *
    autocommit: *
    charset:  *
```
## 打包
`python setup.py sdist bdist`  
`twine upload -r my_nexus dist/*`