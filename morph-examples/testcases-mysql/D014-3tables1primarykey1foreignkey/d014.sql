-- phpMyAdmin SQL Dump
-- version 4.2.7.1
-- http://www.phpmyadmin.net
--
-- Host: 127.0.0.1
-- Generation Time: Dec 17, 2014 at 12:18 PM
-- Server version: 5.6.20
-- PHP Version: 5.5.15

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;

--
-- Database: `d014`
--
CREATE DATABASE IF NOT EXISTS `d014` DEFAULT CHARACTER SET latin1 COLLATE latin1_swedish_ci;
USE `d014`;

-- --------------------------------------------------------

--
-- Table structure for table `dept`
--

CREATE TABLE IF NOT EXISTS `dept` (
  `deptno` int(11) DEFAULT NULL,
  `dname` varchar(30) DEFAULT NULL,
  `loc` varchar(100) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Dumping data for table `dept`
--

INSERT INTO `dept` (`deptno`, `dname`, `loc`) VALUES
(10, 'APPSERVER', 'NEW YORK');

-- --------------------------------------------------------

--
-- Table structure for table `emp`
--

CREATE TABLE IF NOT EXISTS `emp` (
  `empno` int(11) NOT NULL,
  `ename` varchar(100) DEFAULT NULL,
  `job` varchar(30) DEFAULT NULL,
  `deptno` int(11) DEFAULT NULL,
  `etype` varchar(30) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Dumping data for table `emp`
--

INSERT INTO `emp` (`empno`, `ename`, `job`, `deptno`, `etype`) VALUES
(7369, 'SMITH', 'CLERK', 10, 'PART_TIME');

-- --------------------------------------------------------

--
-- Table structure for table `likes`
--

CREATE TABLE IF NOT EXISTS `likes` (
  `id` int(11) DEFAULT NULL,
  `likeType` varchar(30) DEFAULT NULL,
  `likedObj` varchar(100) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Dumping data for table `likes`
--

INSERT INTO `likes` (`id`, `likeType`, `likedObj`) VALUES
(7369, 'Playing', 'Soccer'),
(7369, 'Watching', 'Basketball');

--
-- Indexes for dumped tables
--

--
-- Indexes for table `dept`
--
ALTER TABLE `dept`
 ADD UNIQUE KEY `deptno` (`deptno`);

--
-- Indexes for table `emp`
--
ALTER TABLE `emp`
 ADD PRIMARY KEY (`empno`), ADD KEY `deptno` (`deptno`);

--
-- Constraints for dumped tables
--

--
-- Constraints for table `emp`
--
ALTER TABLE `emp`
ADD CONSTRAINT `emp_ibfk_1` FOREIGN KEY (`deptno`) REFERENCES `dept` (`deptno`);

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
