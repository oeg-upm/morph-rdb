-- phpMyAdmin SQL Dump
-- version 4.2.7.1
-- http://www.phpmyadmin.net
--
-- Host: 127.0.0.1
-- Generation Time: Dec 12, 2014 at 04:51 PM
-- Server version: 5.6.20
-- PHP Version: 5.5.15

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;

--
-- Database: `d016`
--
CREATE DATABASE IF NOT EXISTS `d016` DEFAULT CHARACTER SET latin1 COLLATE latin1_swedish_ci;
USE `d016`;

-- --------------------------------------------------------

--
-- Table structure for table `patient`
--

CREATE TABLE IF NOT EXISTS `patient` (
  `ID` int(11) NOT NULL DEFAULT '0',
  `FirstName` varchar(50) DEFAULT NULL,
  `LastName` varchar(50) DEFAULT NULL,
  `Sex` varchar(6) DEFAULT NULL,
  `Weight` double DEFAULT NULL,
  `Height` float DEFAULT NULL,
  `BirthDate` date DEFAULT NULL,
  `EntranceDate` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `PaidInAdvance` tinyint(1) DEFAULT NULL,
  `Photo` varbinary(200) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Dumping data for table `patient`
--

INSERT INTO `patient` (`ID`, `FirstName`, `LastName`, `Sex`, `Weight`, `Height`, `BirthDate`, `EntranceDate`, `PaidInAdvance`, `Photo`) VALUES
(10, 'Monica', 'Geller', 'female', 80.25, 1.65, '1981-10-10', '2009-10-10 10:12:22', 0, 0x89504e470d0a1a0a0000000d49484452000000050000000508060000008d6f26e50000001c4944415408d763f9fffebfc37f062005c3201284d031f18258cd04000ef535cbd18e0e1f0000000049454e44ae426082),
(11, 'Rachel', 'Green', 'female', 70.22, 1.7, '1982-11-12', '2008-11-12 08:45:44', 1, 0x89504e470d0a1a0a0000000d49484452000000050000000508060000008d6f26e50000001c4944415408d763f9ffff3fc37f062005c3201284d031f18258cd04000ef535cbd18e0e1f0000000049454e44ae426082),
(12, 'Chandler', 'Bing', 'male', 90.31, 1.76, '1978-04-06', '2007-03-12 01:13:14', 1, 0x89504e470d0a1a0a0000000d49484452000000050000000508060000008d6f26e50000001c4944415408d763f9fffebfc37f062005c3201284d031f18258cd04000ef535cbd18e0e1f0000000049454e44ae426082);

--
-- Indexes for dumped tables
--

--
-- Indexes for table `patient`
--
ALTER TABLE `patient`
 ADD PRIMARY KEY (`ID`);

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
