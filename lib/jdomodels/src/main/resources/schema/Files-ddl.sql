CREATE TABLE `FILES` (
  `ID` bigint(20) NOT NULL,
  `ETAG` char(36) NOT NULL,
  `PREVIEW_ID` bigint(20) DEFAULT NULL,
  `CREATED_ON` TIMESTAMP NOT NULL,
  `CREATED_BY` bigint(20) NOT NULL,
  `METADATA_TYPE` ENUM('S3', 'EXTERNAL', 'PREVIEW') NOT NULL,
  `CONTENT_TYPE` varchar(256) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `CONTENT_SIZE` bigint(20) DEFAULT NULL,
  `CONTENT_MD5` varchar(100) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `BUCKET_NAME` varchar(100) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL,
  `NAME` varchar(100) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,
  `KEY` varchar(2000) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL,
  PRIMARY KEY (`ID`),
  CONSTRAINT `PREVEIW_ID_FK` FOREIGN KEY (`PREVIEW_ID`) REFERENCES `FILES` (`ID`) ON DELETE SET NULL,
  CONSTRAINT `FILE_CREATED_BY_FK` FOREIGN KEY (`CREATED_BY`) REFERENCES `JDOUSERGROUP` (`ID`)
)