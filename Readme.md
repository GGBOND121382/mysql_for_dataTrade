# DataTrade数据库

## 应用需求

SQL作为平台最重要的模块之一，需要优先考虑好哪些模块会和SQL产生交互，以便进行SQL的表设计:
- 数据确权后，数据会被记录在SQL中。数据包括:图像、视频、文本、表格和模型数据。用户对这些数据的权属是所有权。
- 用户自己有一个独有的id，是在用户进行注册的时候生成的。
- 数据出售的时候，用户能够查看自己的确权数据。如果用户选择出售数据，则被选择出售的数据会变为可搜索状态;如果用户选择勾选数据以出售数据集，则会直接出售数据集。在买家进行数据检索时，可以选择检索数据，即检索在售的数据。也可以检索数据集，即在售的数据集。
用户购买数据获得使用权后，会获得相应的数据在SQL中，对应的权属是使用权。
- 用户售出数据嵌入水印时，水印会被平台自动嵌入到数据中，并在用户这里记录下水印内容以进行侵权追溯。

因此，SQL的表设计中，每个用户的个人信息包括:一个独特的id和两种类型的数据(数据分为图像、视频、文本、表格，模型数据和数据集，数据集是出售时选择出售数据集而从确权数据中生成的)，一种是具有所有权的数据(即确权数据)，这些数据是用户的同时权属是所有权。这些数据如果交易了使用权，则同时可以看到对应的水印信息。另一种是使用权的数据，这些数据不是用户的，仅享有使用权。此外，这些数据还有出售中和未出售的状态。

## DataTrade数据库结构

数据表：
- **用户** User (ID CHAR(255)): 存储用户ID
- **数据** Data (dataID CHAR(255), dataType INT, datasetID CHAR(255)): 数据ID，数据类型（0：图像，1：文本，2：表格，3：视频，4：模型），数据集ID
  - 添加列dataInfo: 对应**数据** Data (dataID CHAR(255), dataType INT, datasetID CHAR(255), dataInfo CHAR(255))
- **确权数据** ConfirmedData (dataID CHAR(255), userID CHAR(255), isConfirmed BOOL): 数据ID，数据拥有者ID，是否完成确权
- **数据水印** Watermark (authorizedUserID CHAR(255), authorizedDataID CHAR(255), watermarkInfo CHAR(255)): 授权用户ID，授权数据ID，水印信息
- **授权数据** AuthorizedData (dataID CHAR(255), userID CHAR(255), isAuthorized BOOL): 数据ID，授权用户ID，是否完成确权

## SQL语句

建表语句：
- **用户**: CREATE TABLE User (ID CHAR(255) PRIMARY KEY);
- **数据**: CREATE TABLE Data (dataID CHAR(255) PRIMARY KEY, dataType INT, datasetID CHAR(255));
  - 添加列dataInfo: ALTER TABLE DATA ADD dataInfo CHAR(255); 
  - 或直接建表: CREATE TABLE Data (dataID CHAR(255) PRIMARY KEY, dataType INT, datasetID CHAR(255), dataInfo CHAR(255));
- **确权数据**: CREATE TABLE ConfirmedData (dataID CHAR(255) PRIMARY KEY, userID CHAR(255), isConfirmed BOOL);
- **数据水印**: CREATE TABLE Watermark (authorizedUserID CHAR(255), authorizedDataID CHAR(255), watermarkInfo CHAR(255), PRIMARY KEY (authorizedUserID, authorizedDataID));
- **授权数据**: CREATE TABLE AuthorizedData (dataID CHAR(255), userID CHAR(255), isAuthorized BOOL, PRIMARY KEY (dataID, userID));

数据写入语句：
- **加入用户**: INSERT INTO User (ID) VALUES ();
- **数据入库**: INSERT INTO Data (dataID, dataType, datasetID) VALUES ();
- **数据权属写入**: INSERT INTO ConfirmedData (dataID, userID, isConfirmed) VALUES ('dataID', 'userID', True) ON DUPLICATE KEY UPDATE userID='userID', isConfirmed=True;
- **数据集权属写入**: INSERT INTO ConfirmedData (dataID, userID, isConfirmed) SELECT dataID, 'userID' as userID, TRUE as isAuthorized FROM Data WHERE datasetID='datasetID' ON DUPLICATE KEY UPDATE userID='userID', isConfirmed=TRUE;
- **水印入库**: INSERT INTO Watermark (authorizedUserID, authorizedDataID, watermarkInfo) VALUES ();
- **数据授权写入**: INSERT INTO AuthorizedData (dataID, userID, isAuthorized) VALUES ();
- **数据集授权**: INSERT INTO AuthorizedData (dataID, userID, isAuthorized) SELECT dataID, 'userID' as userID, TRUE as isAuthorized FROM Data WHERE datasetID='datasetID' ON DUPLICATE KEY UPDATE isAuthorized=TRUE;

检索语句：
- **用户查询**: SELECT * FROM User WHERE ID=;
- **数据查询**: SELECT * FROM Data WHERE dataID=;
- **数据集查询**: SELECT * FROM Data WHERE datasetID=;
- **数据权属查询**: SELECT * FROM ConfirmedData WHERE dataID= and userID=;
- **数据集权属查询**: SELECT ConfirmedData.dataID, isConfirmed FROM ConfirmedData RIGHT JOIN Data ON ConfirmedData.dataID=Data.dataID and userID= WHERE dataID=;
- **水印查询**: SELECT * FROM Watermark WHERE authorizedUserID= and authorizedDataID=;
- **数据授权查询**: SELECT isAuthorized FROM AuthorizedData where dataID= and userID=;
- **数据集授权查询**: SELECT AuthorizedData.dataID, isAuthorized FROM AuthorizedData RIGHT JOIN Data ON AuthorizedData.dataID=Data.dataID and userID= where datasetID=;