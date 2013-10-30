CREATE TABLE `MESSAGE` (
  `MESSAGE_ID` bigint(20) NOT NULL,
  `THREAD_ID` bigint(20) NOT NULL,
  `CREATED_BY` bigint(20) NOT NULL,
  `RECIPIENT_TYPE` ENUM('ENTITY', 'PRINCIPAL') NOT NULL,
  `RECIPIENTS` blob NOT NULL, 
  `BODY_FILE_ID` bigint(20) NOT NULL,
  `CREATED_ON` datetime NOT NULL,
  `SUBJECT` varchar(256) CHARACTER SET latin1 COLLATE latin1_bin DEFAULT NULL, 
  PRIMARY KEY (`MESSAGE_ID`),
  KEY (`THREAD_ID`),
  CONSTRAINT `SENDER_ID_FK` FOREIGN KEY (`CREATED_BY`) REFERENCES `JDOUSERGROUP` (`ID`),
  CONSTRAINT `FILEHANDLE_ID_FK` FOREIGN KEY (`BODY_FILE_ID`) REFERENCES `FILES` (`ID`)
)