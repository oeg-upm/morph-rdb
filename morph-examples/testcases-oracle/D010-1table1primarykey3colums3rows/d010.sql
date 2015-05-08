-- phpMyAdmin SQL Dump
-- version 4.2.7.1
-- http://www.phpmyadmin.net
--
-- Host: 127.0.0.1
-- Generation Time: Dec 12, 2014 at 04:08 PM
-- Server version: 5.6.20
-- PHP Version: 5.5.15

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;

--
-- Database: `d010`
--
CREATE DATABASE IF NOT EXISTS `d010` DEFAULT CHARACTER SET latin1 COLLATE latin1_swedish_ci;
USE `d010`;

-- --------------------------------------------------------

--
-- Table structure for table `country info`
--

CREATE TABLE IF NOT EXISTS `country info` (
  `Country Code` int(11) NOT NULL,
  `Name` varchar(100) DEFAULT NULL,
  `ISO 3166` varchar(10) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Dumping data for table `country info`
--

INSERT INTO `country info` (`Country Code`, `Name`, `ISO 3166`) VALUES
(1, 'Bolivia, Plurinational State of', 'BO'),
(2, 'Ireland', 'IE'),
(3, 'Saint Martin (French part)', 'MF');

--
-- Indexes for dumped tables
--

--
-- Indexes for table `country info`
--
ALTER TABLE `country info`
 ADD PRIMARY KEY (`Country Code`);

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
