-- phpMyAdmin SQL Dump
-- version 4.0.4.1
-- http://www.phpmyadmin.net
--
-- Host: localhost
-- Generation Time: Nov 13, 2013 at 04:18 
-- Server version: 5.6.12
-- PHP Version: 5.5.3

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;

--
-- Database: `morph_example`
--
CREATE DATABASE IF NOT EXISTS `morph_example` DEFAULT CHARACTER SET latin1 COLLATE latin1_swedish_ci;
USE `morph_example`;

-- --------------------------------------------------------

--
-- Table structure for table `Sport`
--

CREATE TABLE IF NOT EXISTS `Sport` (
  `id` int(11) NOT NULL DEFAULT '0',
  `name` varchar(50) DEFAULT NULL,
  `code` char(8) DEFAULT NULL,
  `type` char(8) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Dumping data for table `Sport`
--

INSERT INTO `Sport` (`id`, `name`, `code`, `type`) VALUES
(100, 'Tennis', 'TNS', 'BOTH'),
(200, 'Chess', 'CHS', 'INDOOR'),
(300, 'Soccer', 'SCR', 'OUTDOOR');

-- --------------------------------------------------------

--
-- Table structure for table `Student`
--

CREATE TABLE IF NOT EXISTS `Student` (
  `id` char(8) NOT NULL DEFAULT '0',
  `name` varchar(50) DEFAULT NULL,
  `sport` int(11) DEFAULT NULL,
  `status` varchar(10) NOT NULL,
  `webpage` varchar(100) DEFAULT NULL,
  `phone` varchar(100) DEFAULT NULL,
  `email` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `Sport` (`sport`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Dumping data for table `Student`
--

INSERT INTO `Student` (`id`, `name`, `sport`, `status`, `webpage`, `phone`, `email`) VALUES
('B1', 'Paul', 100, 'active', NULL, '777-3426', NULL),
('B2', 'John', 200, 'active', NULL, NULL, 'john@acd.edu'),
('B3', 'George', 300, 'active', 'www.george.edu', NULL, NULL),
('B4', 'Ringo', NULL, 'active', 'www.starr.edu', '888-4537', 'ringo@acd.edu');

--
-- Constraints for dumped tables
--

--
-- Constraints for table `Student`
--
ALTER TABLE `Student`
  ADD CONSTRAINT `Student_ibfk_1` FOREIGN KEY (`sport`) REFERENCES `Sport` (`id`);

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
