-- phpMyAdmin SQL Dump
-- version 4.2.7.1
-- http://www.phpmyadmin.net
--
-- Host: 127.0.0.1
-- Generation Time: Dec 12, 2014 at 04:27 PM
-- Server version: 5.6.20
-- PHP Version: 5.5.15

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;

--
-- Database: `d012`
--
CREATE DATABASE IF NOT EXISTS `d012` DEFAULT CHARACTER SET latin1 COLLATE latin1_swedish_ci;
USE `d012`;

-- --------------------------------------------------------

--
-- Table structure for table `ious`
--

CREATE TABLE IF NOT EXISTS `ious` (
  `fname` varchar(20) DEFAULT NULL,
  `lname` varchar(20) DEFAULT NULL,
  `amount` float DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Dumping data for table `ious`
--

INSERT INTO `ious` (`fname`, `lname`, `amount`) VALUES
('Bob', 'Smith', 30),
('Sue', 'Jones', 20),
('Bob', 'Smith', 30);

-- --------------------------------------------------------

--
-- Table structure for table `lives`
--

CREATE TABLE IF NOT EXISTS `lives` (
  `fname` varchar(20) DEFAULT NULL,
  `lname` varchar(20) DEFAULT NULL,
  `city` varchar(20) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Dumping data for table `lives`
--

INSERT INTO `lives` (`fname`, `lname`, `city`) VALUES
('Bob', 'Smith', 'London'),
('Sue', 'Jones', 'Madrid'),
('Bob', 'Smith', 'London');

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
