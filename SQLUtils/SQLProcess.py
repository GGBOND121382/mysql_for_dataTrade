# -*- coding: utf-8 -*-

import pymysql.cursors


def convertItem(item):
    """
    将python数据转换为SQL所需
    Args:
        item: 输入的python数据

    Returns:
        转换后的MySQL输入字符串

    """
    if item is None:
        return "NULL"
    if isinstance(item, str):
        if item.upper() == "NULL":
            return "NULL"
        return "'" + item + "'"
    return repr(item)


def convertRow(row):
    """
    将python行数据转换为SQL所需
    Args:
        row: 输入的python输入数组

    Returns:
        转换后的MySQL输入字符串

    """
    ret = []
    for item in row:
        ret.append(convertItem(item))
    return ",".join(ret)


class SQLProcess:
    """
    SQL操作类

    Attributes:
        host: MySQL地址
        user: MySQL用户名
        password: MySQL密码
        database: MySQL数据库名

    Dependencies:
        pymysql

    """
    def __init__(self, host='localhost',
                 user='root',
                 password='root',
                 database='DataTrade'):
        """
        SQLProcess初始化
        """
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = pymysql.connect(host=host,
                                          user=user,
                                          password=password,
                                          database=database,
                                          cursorclass=pymysql.cursors.DictCursor)

    def runCommand(self, command):
        """
        执行MySQL命令command

        Args:
            command: MySQL命令

        

        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(command)
            self.connection.commit()
        except Exception as e:
            print("Exception: ", e)

    def insert(self, tableName, columnNames, values=None, selectCondition=None, insertSeparately=False,
               judgeDuplicatedKey=False, elseUpdate=""):
        """
        MySQL插入操作

        Args:
            tableName: 目标数据表
            columnNames: 目标列名
            values: 插入的值的二维数组
            selectCondition: 插入的select语句
            insertSeparately: 是否对values的每一行逐行插入
            judgeDuplicatedKey: 是否需判定主键是否重复
            elseUpdate: 如果主键重复，执行elseUpdate更新

        

        """
        if insertSeparately:
            assert values is not None
            for value in values:
                self.insertRow(tableName, columnNames, value=value, judgeDuplicatedKey=judgeDuplicatedKey,
                               elseUpdate=elseUpdate)
        else:
            if values is not None:
                queryString = "INSERT INTO " + tableName + "(" + ",".join(columnNames) + ") VALUES "
                for i in range(len(values)):
                    convertedValue = convertRow(values[i])
                    queryString += "(" + convertedValue + ")"
                    if i < len(values) - 1:
                        queryString += ", "
                if judgeDuplicatedKey:
                    assert elseUpdate != ""
                    queryString += " ON DUPLICATE KEY UPDATE " + elseUpdate
                # queryString += ";"
            else:
                assert selectCondition is not None
                queryString = "INSERT INTO " + tableName + "(" + ",".join(columnNames) + ") " + selectCondition
                if judgeDuplicatedKey:
                    assert elseUpdate != ""
                    queryString += " ON DUPLICATE KEY UPDATE " + elseUpdate
                # queryString += ";"
            print("queryString:", queryString)
            self.runCommand(queryString)

    def insertRow(self, tableName, columnNames, value=None, judgeDuplicatedKey=False, elseUpdate=""):
        """
        插入一行数据

        Args:
            tableName: 目标数据表
            columnNames: 目标列名数组
            value: 插入的值的数组
            judgeDuplicatedKey: 是否需判定主键是否重复
            elseUpdate: 如果主键重复，执行elseUpdate更新

        

        """
        queryString = "INSERT INTO " + tableName + "(" + ",".join(columnNames) + ") VALUES "
        convertedValue = convertRow(value)
        queryString += "(" + convertedValue + ")"
        if judgeDuplicatedKey:
            assert elseUpdate != ""
            queryString += " ON DUPLICATE KEY UPDATE " + elseUpdate
        # queryString += ";"
        # print("queryString:", queryString)
        self.runCommand(queryString)

    def select(self, tableName, columnNames, condition=None, joinCondition=None, notSearch=False):
        """
        MySQL的select操作

        Args:
            tableName: 目标数据表
            columnNames: 目标列名
            condition: select行需满足的条件
            notSearch: 不执行select，只返回查询语句

        Returns:
            (results, queryString)，其中results为查询结果，queryString为查询语句

        """
        # queryString = "INSERT INTO " + tableName + "(" + ",".join(columnNames) + ") VALUES "
        queryString = "SELECT " + ",".join(columnNames) + " FROM " + tableName
        if joinCondition is not None:
            queryString += " " + joinCondition
        if condition is not None:
            queryString += " WHERE " + condition
        try:
            if not notSearch:
                with self.connection.cursor() as cursor:
                    cursor.execute(queryString)
                    results = cursor.fetchall()
            else:
                results = None
            return results, queryString
            # self.connection.commit()
        except Exception as e:
            print("Exception: ", e)
            return None, None

    def update(self, tableName, columnName, value, condition):
        """
        MySQL的update操作

        Args:
            tableName: 目标数据表
            columnName: 目标列名
            value: 插入的值的二维数组
            condition: update行需满足的条件

        

        """
        value = convertItem(value)
        queryString = "UPDATE " + tableName + " SET " + columnName + " = " + value + " WHERE " + condition
        self.runCommand(queryString)

    def delete(self, tableName, condition):
        """
        MySQL的删除操作

        Args:
            tableName: 目标数据表
            condition: delete行需满足的条件

        

        """
        queryString = "DELETE FROM " + tableName + " WHERE " + condition
        self.runCommand(queryString)

    def disconnect(self):
        """
        断开MySQL连接

        """
        self.connection.close()


if __name__ == '__main__':
    sqlProcess = SQLProcess()
    # sqlProcess.runCommand("INSERT INTO `User` (`ID`) VALUES ('BA00000002')")
    tableName = "User"
    columnNames = ["ID", ]
    values = [["BA00000003", ], ["BA00000004", ], ["BA00000005", ]]
    sqlProcess.insert(tableName, columnNames, values=values, insertSeparately=True)
    results, query = sqlProcess.select(tableName, columnNames)
    print("Selection results: ", results)
    print("Query: ", query)

    tableName = "Data"
    columnNames = ["dataID", "dataType", "datasetID"]
    values = [["D00000001", 0, "DS00000001"], ["D00000002", 1, "DS00000001"],
              ["D00000003", 2, "DS00000003"], ["D00000004", 3, "DS00000003"],
              ["D00000005", 4, "DS00000004"], ["D00000006", 4, None],
              ["D00000007", 4, "null"]]
    sqlProcess.insert(tableName, columnNames, values=values, insertSeparately=True)
    results, _ = sqlProcess.select(tableName, columnNames)
    print("Selection results: ", results)

    tableName = "Watermark"
    columnNames = ["authorizedUserID", "authorizedDataID", "watermarkInfo"]
    values = [["BA00000001", "D00000001", None], ["BA00000002", "D00000001", None],
              ["BA00000003", "D00000003", None]]
    sqlProcess.insert(tableName, columnNames, values=values, insertSeparately=True)
    results, _ = sqlProcess.select(tableName, columnNames)
    print("Selection results: ", results)

    tableName = "ConfirmedData"
    columnNames = ["dataID", "userID", "isConfirmed"]
    values = [["D00000001", "BA00000001", False], ["D00000002", "BA00000002", False],
              ["D00000003", "BA00000003", True]]
    sqlProcess.insert(tableName, columnNames, values=values, insertSeparately=True, judgeDuplicatedKey=True, elseUpdate="isConfirmed = True")
    results, _ = sqlProcess.select(tableName, columnNames)
    print("Selection results: ", results)

    tableName = "AuthorizedData"
    columnNames = ["dataID", "userID", "isAuthorized"]
    values = [["D00000001", "BA00000001", False], ["D00000002", "BA00000002", False],
              ["D00000003", "BA00000003", True]]
    sqlProcess.insert(tableName, columnNames, values=values, insertSeparately=True)
    results, _ = sqlProcess.select(tableName, columnNames)
    print("Selection results: ", results)

    tableName = "AuthorizedData"
    columnName = "isAuthorized"
    sqlProcess.update(tableName, columnName, True, "dataID = 'D00000001'")

    tableName = "AuthorizedData"
    columnName = "isAuthorized"
    sqlProcess.delete(tableName, "dataID = 'D00000002'")

    sqlProcess.disconnect()
