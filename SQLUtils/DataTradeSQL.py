# -*- coding: utf-8 -*-

import SQLProcess
import numpy as np


class DataTradeSQL:
    """
    DataTrade操作类

    Attributes:
        host: MySQL数据库地址
        user: MySQL用户名
        password: MySQL密码
        database: DataTrade数据库名

    Dependencies:
        pymysql
        numpy

    """

    # Methods:
    #     addUser: 添加用户
    #     addUsers: 添加多个用户
    #     addData: 添加数据
    #     addDatas: 添加多条数据
    #     addConfirmedData: 添加确权数据
    #     addConfirmedDatas: 添加多条确权数据
    #     addConfirmedDataset: 添加确权数据集
    #     addWatermark: 添加水印数据
    #     addWatermarks: 添加多条水印
    #     addAuthorizedData: 添加授权数据
    #     addAuthorizedDatas: 添加多条授权数据
    #     addAuthorizedDataset: 添加授权数据集
    #     queryUser: 查询用户数据表User
    #     queryData: 查询数据表Data中dataID对应数据
    #     queryDataset: 查询Data数据表中的数据集
    #     queryWatermark: 查询Watermark数据表中记录
    #     queryAuthorizedData: 查询AuthorizedData中授权数据的记录
    #     queryAuthorizedDataset: 查询AuthorizedData中授权数据的记录
    #     queryConfirmedData: 查询ConfirmedData中确权数据的记录
    #     queryConfirmedDataset: 查询ConfirmedData中确权数据的记录
    #     disconnect: 断开MySQL连接

    def __init__(self, host='localhost',
                 user='root',
                 password='root',
                 database='DataTrade'):
        """
        DataTradeSQL初始化函数，创建与DataTrade数据库的连接
        """
        self.sqlProcess = SQLProcess.SQLProcess(host, user, password, database)

    def addUser(self, ID):
        """
        添加用户

        Args:
            ID: 用户ID

        

        """
        self.sqlProcess.insertRow('User', ['ID', ], [ID, ])

    def addUsers(self, IDs):
        """
        添加多个用户

        Args:
            IDs: 多用户ID构成的数组

        

        """
        IDs = np.reshape(IDs, (-1, 1))
        IDs = IDs.tolist()
        self.sqlProcess.insert('User', ['ID', ], IDs)

    def addData(self, dataID, dataType, datasetID=None, dataInfo=None):
        """
        添加数据

        Args:
            dataID: 数据ID
            dataType: 数据类型（0：图像，1：文本，2：表格，3：视频，4：模型）
            datasetID: 数据集ID
            dataInfo: 数据额外信息

        

        """
        self.sqlProcess.insertRow('Data',
                                  ['dataID', 'dataType', 'datasetID', 'dataInfo'],
                                  [dataID, dataType, datasetID, dataInfo])

    def addDatas(self, dataIDs, dataTypes, datasetIDs=None, dataInfos=None):
        """
        添加多条数据

        Args:
            dataIDs: 数据ID数组
            dataTypes: 数据类型数组
            datasetIDs: 数据集ID数组
            dataInfos: 数据额外信息数组

        

        """
        assert len(dataIDs) == len(dataTypes)
        if datasetIDs is None:
            datasetIDs = [None] * len(dataIDs)
        if dataInfos is None:
            dataInfos = [None] * len(dataIDs)
        assert len(dataIDs) == len(datasetIDs)
        assert len(dataIDs) == len(dataInfos)

        dataIDs = np.reshape(dataIDs, (-1, 1))
        dataTypes = np.reshape(dataTypes, (-1, 1))
        datasetIDs = np.reshape(datasetIDs, (-1, 1))
        dataInfos = np.reshape(dataInfos, (-1, 1))
        dataIDs = np.array(dataIDs, dtype=object)
        dataTypes = np.array(dataTypes, dtype=object)
        datasetIDs = np.array(datasetIDs, dtype=object)
        dataInfos = np.array(dataInfos, dtype=object)
        rowsToInsert = np.hstack((dataIDs, dataTypes, datasetIDs, dataInfos))
        rowsToInsert = rowsToInsert.tolist()
        self.sqlProcess.insert('Data',
                               ['dataID', 'dataType', 'datasetID', 'dataInfo'],
                               rowsToInsert)

    def addConfirmedData(self, dataID, userID, isConfirmed=False):
        """
        添加确权数据

        Args:
            dataID: 数据ID
            userID: 用户ID
            isConfirmed: 是否确权（默认为False）

        

        """
        self.sqlProcess.insertRow('ConfirmedData',
                                  ['dataID', 'userID', 'isConfirmed'],
                                  [dataID, userID, isConfirmed],
                                  judgeDuplicatedKey=True,
                                  elseUpdate="isConfirmed = VALUES(isConfirmed), userID = VALUES(userID)"
                                  )

    def addConfirmedDatas(self, dataIDs, userIDs, isConfirmeds=None):
        """
        添加多条确权数据

        Args:
            dataIDs: 数据ID数组
            userIDs: 用户ID数组
            isConfirmeds: 是够确权数组（默认为False）

        

        """
        assert len(dataIDs) == len(userIDs)
        if isConfirmeds is None:
            isConfirmeds = [None] * len(dataIDs)
        assert len(dataIDs) == len(isConfirmeds)

        dataIDs = np.reshape(dataIDs, (-1, 1))
        userIDs = np.reshape(userIDs, (-1, 1))
        isConfirmeds = np.reshape(isConfirmeds, (-1, 1))
        dataIDs = np.array(dataIDs, dtype=object)
        userIDs = np.array(userIDs, dtype=object)
        isConfirmeds = np.array(isConfirmeds, dtype=object)
        rowsToInsert = np.hstack((dataIDs, userIDs, isConfirmeds))
        # print(rowsToInsert)
        rowsToInsert = rowsToInsert.tolist()
        self.sqlProcess.insert('ConfirmedData',
                               ['dataID', 'userID', 'isConfirmed'],
                               rowsToInsert,
                               judgeDuplicatedKey=True,
                               # elseUpdate="isConfirmed = " + repr(isConfirmed) + ", " +
                               #            "userID = " + SQLProcess.convertItem(userID)
                               elseUpdate="isConfirmed = VALUES(isConfirmed), userID = VALUES(userID)"
                               )

    def addConfirmedDataset(self, datasetID, userID, isConfirmed=False):
        """
        添加确权数据集

        Args:
            datasetID: 数据集ID
            userID: 用户ID
            isConfirmed: 是否确权（默认为False）

        

        """
        self.sqlProcess.insert('ConfirmedData',
                               ['dataID', 'userID', 'isConfirmed'],
                               selectCondition="SELECT dataID, " + SQLProcess.convertItem(userID) +
                                               " as userID, " + repr(isConfirmed) +
                                               " as isConfirmed FROM Data WHERE datasetID=" +
                                               SQLProcess.convertItem(datasetID),
                               judgeDuplicatedKey=True,
                               elseUpdate="isConfirmed = " + repr(isConfirmed) + ", " +
                                          "userID = " + SQLProcess.convertItem(userID)
                               )

    def addWatermark(self, authorizedUserID, authorizedDataID, watermarkInfo=None):
        """
        添加水印数据

        Args:
            authorizedUserID: 授权用户ID
            authorizedDataID: 授权数据ID
            watermarkInfo: 水印信息

        

        """
        self.sqlProcess.insertRow('Watermark',
                                  ['authorizedUserID', 'authorizedDataID', 'watermarkInfo'],
                                  [authorizedUserID, authorizedDataID, watermarkInfo])

    def addWatermarks(self, authorizedUserIDs, authorizedDataIDs, watermarkInfos=None):
        """
        添加多条水印

        Args:
            authorizedUserIDs: 授权用户ID数组
            authorizedDataIDs: 授权数据ID数组
            watermarkInfos: 水印信息数组

        

        """
        assert len(authorizedUserIDs) == len(authorizedDataIDs)
        if watermarkInfos is None:
            watermarkInfos = [None] * len(authorizedUserIDs)
        assert len(watermarkInfos) == len(authorizedUserIDs)

        authorizedUserIDs = np.reshape(authorizedUserIDs, (-1, 1))
        authorizedDataIDs = np.reshape(authorizedDataIDs, (-1, 1))
        watermarkInfos = np.reshape(watermarkInfos, (-1, 1))
        rowsToInsert = np.hstack((authorizedUserIDs, authorizedDataIDs, watermarkInfos))
        rowsToInsert = rowsToInsert.tolist()
        self.sqlProcess.insert('Watermark',
                               ['authorizedUserID', 'authorizedDataID', 'watermarkInfo'],
                               rowsToInsert)

    def addAuthorizedData(self, dataID, userID, isAuthorized=False):
        """
        添加授权数据

        Args:
            dataID: 数据ID
            userID: 用户ID
            isAuthorized: 是否授权（默认为False）

        

        """
        self.sqlProcess.insertRow('AuthorizedData',
                                  ['dataID', 'userID', 'isAuthorized'],
                                  [dataID, userID, isAuthorized])

    def addAuthorizedDatas(self, dataIDs, userIDs, isAuthorizeds=None):
        """
        添加多条授权数据

        Args:
            dataIDs: 数据ID数组
            userIDs: 用户ID数组
            isAuthorizeds: 是否授权数组（默认为False）

        

        """
        assert len(dataIDs) == len(userIDs)
        if isAuthorizeds is None:
            isAuthorizeds = [None] * len(dataIDs)
        assert len(isAuthorizeds) == len(dataIDs)

        dataIDs = np.reshape(dataIDs, (-1, 1))
        userIDs = np.reshape(userIDs, (-1, 1))
        isAuthorizeds = np.reshape(isAuthorizeds, (-1, 1))
        dataIDs = np.array(dataIDs, dtype=object)
        userIDs = np.array(userIDs, dtype=object)
        isAuthorizeds = np.array(isAuthorizeds, dtype=object)
        rowsToInsert = np.hstack((dataIDs, userIDs, isAuthorizeds))
        rowsToInsert = rowsToInsert.tolist()
        self.sqlProcess.insert('AuthorizedData',
                               ['dataID', 'userID', 'isAuthorized'],
                               rowsToInsert)

    def addAuthorizedDataset(self, datasetID, userID, isAuthorized=False):
        """
        添加授权数据集

        Args:
            datasetID: 数据集ID
            userID: 用户ID
            isAuthorized: 是否授权（默认为False）

        

        """
        self.sqlProcess.insert('AuthorizedData',
                               ['dataID', 'userID', 'isAuthorized'],
                               selectCondition="SELECT dataID, " + SQLProcess.convertItem(userID) +
                                               " as userID, " + repr(isAuthorized) +
                                               " as isAuthorized FROM Data WHERE datasetID=" +
                                               SQLProcess.convertItem(datasetID),
                               judgeDuplicatedKey=True,
                               elseUpdate="isAuthorized = " + repr(isAuthorized)
                               )

    def queryUser(self, ID=None):
        """
        查询用户数据表User

        Args:
            ID: 待查询的用户ID，若为空，查询所有ID

        Returns:
            返回User中的查询结果ID

        """
        if ID is not None:
            result, _ = self.sqlProcess.select('User', ['ID', ], condition="ID = " + SQLProcess.convertItem(ID))
        else:
            result, _ = self.sqlProcess.select('User', ['ID', ])
        return result

    def queryData(self, dataID=None, columnNames=None):
        """
        查询数据表Data中dataID对应数据

        Args:
            dataID: 待查询的数据ID，若为空，查询所有ID
            columnNames: 待查询的列名数组，若为空，查询所有列

        Returns:
            返回Data中的查询结果

        """
        if dataID is None and columnNames is None:
            result, _ = self.sqlProcess.select('Data', ['*', ])
        elif dataID is None:
            result, _ = self.sqlProcess.select('Data', columnNames)
        elif columnNames is None:
            result, _ = self.sqlProcess.select('Data', ['*', ], condition="dataID = " + SQLProcess.convertItem(dataID))
        else:
            result, _ = self.sqlProcess.select('Data', columnNames,
                                               condition="dataID = " + SQLProcess.convertItem(dataID))
        return result

    def queryDataset(self, datasetID=None, columnNames=None):
        """
        查询Data数据表中的数据集

        Args:
            datasetID: 待查询的数据集ID，若为空，查询所有ID
            columnNames: 待查询的列名数组，若为空，查询所有列

        Returns:
            返回Data中的数据集查询结果

        """
        if datasetID is None and columnNames is None:
            result, _ = self.sqlProcess.select('Data', ['*', ])
        elif datasetID is None:
            result, _ = self.sqlProcess.select('Data', columnNames)
        elif columnNames is None:
            result, _ = self.sqlProcess.select('Data', ['*', ],
                                               condition="datasetID = " + SQLProcess.convertItem(datasetID))
        else:
            result, _ = self.sqlProcess.select('Data', columnNames,
                                               condition="datasetID = " + SQLProcess.convertItem(datasetID))
        return result

    def queryWatermark(self, authorizedUserID=None, authorizedDataID=None, columnNames=None):
        """
        查询Watermark数据表中记录

        Args:
            authorizedUserID: 待查询的授权用户的ID，若为空，查询所有ID
            authorizedDataID: 待查询的授权数据的ID，若为空，查询所有ID
            columnNames: 待查询的列名数组，若为空，查询所有列

        Returns:
            返回Watermark数据表中查询结果

        """
        if authorizedUserID is None and authorizedDataID is None and columnNames is None:
            result, _ = self.sqlProcess.select('Watermark', ['*', ])
        elif columnNames is None:
            conditionList = []
            if authorizedUserID is not None:
                conditionList.append("authorizedUserID=" + SQLProcess.convertItem(authorizedUserID))
            if authorizedDataID is not None:
                conditionList.append("authorizedDataID=" + SQLProcess.convertItem(authorizedDataID))
            condition = " and ".join(conditionList)
            result, _ = self.sqlProcess.select('Data', ['*', ], condition=condition)
        elif authorizedUserID is None and authorizedDataID is None:
            result, _ = self.sqlProcess.select('Watermark', columnNames)
        else:
            conditionList = []
            if authorizedUserID is not None:
                conditionList.append("authorizedUserID=" + SQLProcess.convertItem(authorizedUserID))
            if authorizedDataID is not None:
                conditionList.append("authorizedDataID=" + SQLProcess.convertItem(authorizedDataID))
            condition = " and ".join(conditionList)
            result, _ = self.sqlProcess.select('Data', columnNames, condition=condition)
        return result

    def queryAuthorizedData(self, dataID=None, userID=None, columnNames=None):
        """
        查询AuthorizedData中授权数据的记录

        Args:
            dataID: 待查询的数据ID，若为空，查询所有ID
            userID: 待查询的用户ID，若为空，查询所有ID
            columnNames: 待查询的列名数组，若为空，查询所有列

        Returns:
            返回AuthorizedData数据表中查询结果

        """
        if dataID is None and userID is None and columnNames is None:
            result, _ = self.sqlProcess.select('AuthorizedData', ['*', ])
        elif columnNames is None:
            conditionList = []
            if dataID is not None:
                conditionList.append("dataID=" + SQLProcess.convertItem(dataID))
            if userID is not None:
                conditionList.append("userID=" + SQLProcess.convertItem(userID))
            condition = " and ".join(conditionList)
            result, _ = self.sqlProcess.select('AuthorizedData', ['*', ], condition=condition)
        elif dataID is None and userID is None:
            result, _ = self.sqlProcess.select('AuthorizedData', columnNames)
        else:
            conditionList = []
            if dataID is not None:
                conditionList.append("dataID=" + SQLProcess.convertItem(dataID))
            if userID is not None:
                conditionList.append("userID=" + SQLProcess.convertItem(userID))
            condition = " and ".join(conditionList)
            result, _ = self.sqlProcess.select('AuthorizedData', columnNames, condition=condition)
        return result

    def queryAuthorizedDataset(self, userID, datasetID=None, columnNames=None):
        """
        查询AuthorizedData中授权数据的记录

        Args:
            datasetID: 待查询的数据集ID，若为空，查询所有ID
            userID: 待查询的用户ID，若为空，查询所有ID
            columnNames: 待查询的列名数组，若为空，查询所有列

        Returns:
            返回AuthorizedData数据表中查询结果

        """
        joinCondition = "RIGHT JOIN Data ON AuthorizedData.dataID=Data.dataID and userID=" + \
                        SQLProcess.convertItem(userID)
        if datasetID is None and columnNames is None:
            result, _ = self.sqlProcess.select('AuthorizedData', ['*', ], joinCondition=joinCondition)
        elif columnNames is None:
            condition = "datasetID=" + SQLProcess.convertItem(datasetID)
            result, _ = self.sqlProcess.select('AuthorizedData', ['*', ],
                                               joinCondition=joinCondition, condition=condition)
        elif datasetID is None:
            result, _ = self.sqlProcess.select('AuthorizedData', columnNames, joinCondition=joinCondition)
        else:
            condition = "datasetID=" + SQLProcess.convertItem(datasetID)
            result, _ = self.sqlProcess.select('AuthorizedData', columnNames,
                                               joinCondition=joinCondition, condition=condition)
        return result

    def queryConfirmedData(self, dataID=None, userID=None, columnNames=None):
        """
        查询ConfirmedData中确权数据的记录

        Args:
            dataID: 待查询的数据ID，若为空，查询所有ID
            userID: 待查询的用户ID，若为空，查询所有ID
            columnNames: 待查询的列名数组，若为空，查询所有列

        Returns:
            返回ConfirmedData数据表中查询结果

        """
        if dataID is None and userID is None and columnNames is None:
            result, _ = self.sqlProcess.select('ConfirmedData', ['*', ])
        elif columnNames is None:
            conditionList = []
            if dataID is not None:
                conditionList.append("dataID=" + SQLProcess.convertItem(dataID))
            if userID is not None:
                conditionList.append("userID=" + SQLProcess.convertItem(userID))
            condition = " and ".join(conditionList)
            result, _ = self.sqlProcess.select('ConfirmedData', ['*', ], condition=condition)
        elif dataID is None and userID is None:
            result, _ = self.sqlProcess.select('ConfirmedData', columnNames)
        else:
            conditionList = []
            if dataID is not None:
                conditionList.append("dataID=" + SQLProcess.convertItem(dataID))
            if userID is not None:
                conditionList.append("userID=" + SQLProcess.convertItem(userID))
            condition = " and ".join(conditionList)
            result, _ = self.sqlProcess.select('ConfirmedData', columnNames, condition=condition)
        return result

    def queryConfirmedDataset(self, userID, datasetID=None, columnNames=None):
        """
        查询ConfirmedData中确权数据的记录

        Args:
            datasetID: 待查询的数据集ID，若为空，查询所有ID
            userID: 待查询的用户ID，若为空，查询所有ID
            columnNames: 待查询的列名数组，若为空，查询所有列

        Returns:
            返回ConfirmedData数据表中查询结果

        """
        joinCondition = "RIGHT JOIN Data ON ConfirmedData.dataID=Data.dataID and userID=" + \
                        SQLProcess.convertItem(userID)
        if datasetID is None and columnNames is None:
            result, _ = self.sqlProcess.select('ConfirmedData', ['*', ], joinCondition=joinCondition)
        elif columnNames is None:
            condition = "datasetID=" + SQLProcess.convertItem(datasetID)
            result, _ = self.sqlProcess.select('ConfirmedData', ['*', ],
                                               joinCondition=joinCondition, condition=condition)
        elif datasetID is None:
            result, _ = self.sqlProcess.select('ConfirmedData', columnNames, joinCondition=joinCondition)
        else:
            condition = "datasetID=" + SQLProcess.convertItem(datasetID)
            result, _ = self.sqlProcess.select('ConfirmedData', columnNames,
                                               joinCondition=joinCondition, condition=condition)
        return result

    def disconnect(self):
        """
        断开MySQL连接

        

        """
        self.sqlProcess.disconnect()


if __name__ == '__main__':
    dataTradeSQL = DataTradeSQL()
    dataTradeSQL.addDatas(['D00000008', 'D00000009', 'D00000010'], [0, 0, 0], ['DS00000002', 'DS00000002', 'DS00000002'])
    dataTradeSQL.addAuthorizedDataset('DS00000004', 'BA00000004')
    dataTradeSQL.addAuthorizedDataset('DS00000001', 'BA00000004')

    dataTradeSQL.addUser('BA00000006')
    dataTradeSQL.addConfirmedDataset('DS00000001', 'BA00000006', True)

    dataTradeSQL.addUsers(['BA00000007', 'BA00000008'])
    dataTradeSQL.addConfirmedData('D00000002', 'BA00000005', False)
    dataTradeSQL.addConfirmedDatas(['D00000008', 'D00000001'], ['BA00000007', 'BA00000008'], [False, True])

    dataTradeSQL.addConfirmedDataset('DS00000001', 'BA00000008', True)
    result = dataTradeSQL.queryConfirmedDataset('BA00000008', 'DS00000001', ['ConfirmedData.dataID', 'isConfirmed'])
    print(result)

    result = dataTradeSQL.queryAuthorizedDataset('BA00000001', 'DS00000001', ['AuthorizedData.dataID', 'isAuthorized'])
    print(result)

    dataTradeSQL.sqlProcess.delete('Data', 'dataInfo IS NULL')

    dataTradeSQL.disconnect()
