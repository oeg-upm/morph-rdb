/*
Navicat MySQL Data Transfer

Source Server         : local
Source Server Version : 50525
Source Host           : localhost:3306
Source Database       : d017b

Target Server Type    : MYSQL
Target Server Version : 50525
File Encoding         : 65001

Date: 2015-01-17 09:27:40
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for `成分`
-- ----------------------------
DROP TABLE IF EXISTS `成分`;
CREATE TABLE `成分` (
  `皿` varchar(10) DEFAULT NULL,
  `植物名` varchar(10) DEFAULT NULL,
  `使用部` varchar(10) DEFAULT NULL,
  KEY `成分` (`植物名`,`使用部`),
  CONSTRAINT `成分` FOREIGN KEY (`植物名`, `使用部`) REFERENCES `植物` (`名`, `使用部`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of 成分
-- ----------------------------
INSERT INTO `成分` VALUES ('しそのとまと', 'しそ', '葉');

-- ----------------------------
-- Table structure for `植物`
-- ----------------------------
DROP TABLE IF EXISTS `植物`;
CREATE TABLE `植物` (
  `名` varchar(10) NOT NULL,
  `使用部` varchar(10) NOT NULL,
  `条件` varchar(10) DEFAULT NULL,
  PRIMARY KEY (`名`,`使用部`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of 植物
-- ----------------------------
INSERT INTO `植物` VALUES ('しそ', '葉', '新鮮な');
