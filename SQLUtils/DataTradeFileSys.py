# -*- coding: utf-8 -*-

import SQLProcess
import DataTradeSQL
import numpy as np


class DataTradeFileSys:
    """
    DataTradeFileSys初始化函数，创建与DataTrade数据库的连接

    Attributes:
        host: MySQL数据库地址
        user: MySQL用户名
        password: MySQL密码
        database: DataTrade数据库名

    Dependencies:
        pymysql
        numpy

    Notes:
        现有文件读写使用Python默认格式，不依赖于文件类型，后续可能需结合平台的文件处理需求调整文件读写的格式
    """

    def __init__(self, host='localhost',
                 user='root',
                 password='root',
                 database='DataTrade'):
        self.dataTradeSQL = DataTradeSQL.DataTradeSQL(host, user, password, database)

    def addData(self, content, dataID, dataType, datasetID=None, dataInfo=None, path='../FileSys/Data'):
        """
        在文件系统中存入数据文件，并在SQL数据库中存入对应的数据描述信息

        Args:
            content: 数据内容
            dataID: 数据ID
            dataType: 数据类型（0：图像，1：文本，2：表格，3：视频，4：模型）
            datasetID: 数据集ID
            dataInfo: 数据额外信息
            path: 待写入的文件夹路径

        

        """
        self.dataTradeSQL.addData(dataID, dataType, datasetID, dataInfo)

        if path.endswith('/'):
            wholePath = path + dataID
        else:
            wholePath = path + '/' + dataID
        with open(wholePath, 'w') as fd:
            fd.write(content)

    def addDatas(self, contents, dataIDs, dataTypes, datasetIDs=None, dataInfos=None, path='../FileSys/Data'):
        """
        在文件系统中存入多个数据文件，并在SQL数据库中存入对应的多个数据描述信息

        Args:
            contents: 数据内容数组
            dataIDs: 数据ID数组
            dataTypes: 数据类型数组
            datasetIDs: 数据集ID数组
            dataInfos: 数据描述信息数组
            path: 待写入的文件夹路径

        

        """
        assert len(contents) == len(dataIDs) and len(contents) == len(dataTypes)

        self.dataTradeSQL.addDatas(dataIDs, dataTypes, datasetIDs, dataInfos)

        for i in range(len(contents)):
            if path.endswith('/'):
                wholePath = path + dataIDs[i]
            else:
                wholePath = path + '/' + dataIDs[i]
            with open(wholePath, 'w') as fd:
                fd.write(contents[i])

    def addWatermark(self, content, authorizedUserID, authorizedDataID,
                     watermarkInfo=None, path='../FileSys/Watermark'):
        """
        在文件系统中存入水印文件，并在SQL数据库中存入对应的水印描述信息

        Args:
            content: 水印内容
            authorizedUserID: 授权用户ID
            authorizedDataID: 授权数据ID
            watermarkInfo: 水印信息
            path: 待写入的文件夹路径

        """
        self.dataTradeSQL.addWatermark(authorizedUserID, authorizedDataID, watermarkInfo)

        watermarkID = authorizedUserID + '_' + authorizedDataID
        if path.endswith('/'):
            wholePath = path + watermarkID
        else:
            wholePath = path + '/' + watermarkID
        with open(wholePath, 'w') as fd:
            fd.write(content)

    def addWatermarks(self, contents, authorizedUserIDs, authorizedDataIDs,
                      watermarkInfos=None, path='../FileSys/Watermark'):
        """
        在文件系统中存入多个水印文件，并在SQL数据库中存入对应的多个水印描述信息

        Args:
            contents: 水印内容数组
            authorizedUserIDs: 授权用户ID数组
            authorizedDataIDs: 授权数据ID数组
            watermarkInfos: 水印信息数组
            path: 待写入的文件夹路径

        

        """
        assert len(contents) == len(authorizedUserIDs) and len(contents) == len(authorizedDataIDs)

        self.dataTradeSQL.addWatermarks(authorizedUserIDs, authorizedDataIDs, watermarkInfos)

        for i in range(len(contents)):
            watermarkID = authorizedUserIDs[i] + '_' + authorizedDataIDs[i]
            if path.endswith('/'):
                wholePath = path + watermarkID
            else:
                wholePath = path + '/' + watermarkID
            with open(wholePath, 'w') as fd:
                fd.write(contents[i])

    def queryData(self, dataID=None, columnNames=None, path='../FileSys/Data'):
        """
        查询数据表Data中dataID对应数据

        Args:
            dataID: 待查询的数据ID，若为空，查询所有ID
            columnNames: 待查询的列名数组，若为空，查询所有列
            path: 待查询的文件夹路径

        Returns:
            (results, contents)，其中results为SQL中columNames对应的数据描述信息数组，contents为查询到的数据内容数组

        """
        try:
            if dataID is not None:
                if path.endswith('/'):
                    wholePath = path + dataID
                else:
                    wholePath = path + '/' + dataID
                result = self.dataTradeSQL.queryData(dataID, columnNames)

                with open(wholePath, 'r') as fd:
                    content = fd.read()
                return [result, ], [content, ]
            else:
                dataIDs = self.dataTradeSQL.queryData(dataID, ['dataID', ])
                contents = []
                for dataID in np.reshape(dataIDs, (len(dataIDs),)):
                    if path.endswith('/'):
                        wholePath = path + dataID
                    else:
                        wholePath = path + '/' + dataID
                    with open(wholePath, 'r') as fd:
                        content = fd.read()
                        contents.append(content)
                results = self.dataTradeSQL.queryData(dataID, columnNames)
                return results, contents
        except Exception as e:
            print("Exception: ", e)
            return None, None

    def queryDataset(self, datasetID=None, columnNames=None, path='../FileSys/Data'):
        """
        查询数据表Data中datasetID对应数据

        Args:
            datasetID: 待查询的数据集ID，若为空，查询所有ID
            columnNames: 待查询的列名数组，若为空，查询所有列
            path: 待查询的文件夹路径

        Returns:
            (results, contents)，其中results为SQL中columNames对应的数据描述信息数组，contents为查询到的数据内容数组

        """
        dataIDs = self.dataTradeSQL.queryDataset(datasetID, ['dataID', ])
        contents = []
        for dataID in np.reshape(dataIDs, (len(dataIDs),)):
            if path.endswith('/'):
                wholePath = path + dataID
            else:
                wholePath = path + '/' + dataID
            with open(wholePath, 'r') as fd:
                content = fd.read()
                contents.append(content)

        results = self.dataTradeSQL.queryDataset(datasetID, columnNames)
        return results, contents

    def queryWatermark(self, authorizedUserID=None, authorizedDataID=None, columnNames=None, path='../FileSys/Data'):
        """
        查询数据表Watermark中authorizedUserID与authorizedDataID对应数据

        Args:
            authorizedUserID: 待查询的授权用户的ID，若为空，查询所有ID
            authorizedDataID: 待查询的授权数据的ID，若为空，查询所有ID
            columnNames: 待查询的列名数组，若为空，查询所有列
            path: 待查询的文件夹路径

        Returns:
            (results, contents)，其中results为SQL中columNames对应的水印描述信息数组，contents为查询到的水印内容数组

        """
        if authorizedUserID is not None and authorizedDataID is not None:
            watermarkID = authorizedUserID + '_' + authorizedDataID
            if path.endswith('/'):
                wholePath = path + watermarkID
            else:
                wholePath = path + '/' + watermarkID
            result = self.dataTradeSQL.queryWatermark(authorizedUserID, authorizedDataID, columnNames)

            with open(wholePath, 'r') as fd:
                content = fd.read()
            return [result, ], [content, ]
        else:
            authorizedUserDataIDs = self.dataTradeSQL.queryWatermark(authorizedUserID, authorizedDataID,
                                                                     ['authorizedUserID', 'authorizedDataID'])
            contents = []
            for userID, dataID in authorizedUserDataIDs:
                watermarkID = userID + '_' + dataID
                if path.endswith('/'):
                    wholePath = path + watermarkID
                else:
                    wholePath = path + '/' + watermarkID
                with open(wholePath, 'r') as fd:
                    content = fd.read()
                    contents.append(content)
            results = self.dataTradeSQL.queryWatermark(authorizedUserID, authorizedDataID, columnNames)
            return results, contents

    def disconnect(self):
        """
        断开MySQL连接

        """
        self.dataTradeSQL.disconnect()


if __name__ == '__main__':
    dataTradeFS = DataTradeFileSys()

    dataTradeFS.addDatas(['test 1', 'text 2', 'test 3', 'text 4', 'test 5', 'text 6',
                          'test 7', 'text 8', 'test 9', 'text 10'],
                         ['D00000001', 'D00000002', 'D00000003', 'D00000004', 'D00000005', 'D00000006',
                          'D00000007', 'D00000008', 'D00000009', 'D00000010'],
                         [0, 1, 2, 3, 4, 4, 4, 0, 0, 0],
                         ['DS00000001', 'DS00000001', 'DS00000003', 'DS00000003', 'DS00000004', 'null',
                          'null', 'DS00000002', 'DS00000002', 'DS00000002'])

    # r, c = dataTradeFS.queryData('D00000001')

    dataTradeFS.disconnect()
