-- phpMyAdmin SQL Dump
-- version 4.2.7.1
-- http://www.phpmyadmin.net
--
-- Host: 127.0.0.1
-- Generation Time: Dec 12, 2014 at 04:25 PM
-- Server version: 5.6.20
-- PHP Version: 5.5.15

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;

--
-- Database: `d011`
--
CREATE DATABASE IF NOT EXISTS `d011` DEFAULT CHARACTER SET latin1 COLLATE latin1_swedish_ci;
USE `d011`;

-- --------------------------------------------------------

--
-- Table structure for table `sport`
--

CREATE TABLE IF NOT EXISTS `sport` (
  `ID` int(11) NOT NULL,
  `Description` varchar(50) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Dumping data for table `sport`
--

INSERT INTO `sport` (`ID`, `Description`) VALUES
(110, 'Tennis'),
(111, 'Football'),
(112, 'Formula1');

-- --------------------------------------------------------

--
-- Table structure for table `student`
--

CREATE TABLE IF NOT EXISTS `student` (
  `ID` int(11) NOT NULL,
  `FirstName` varchar(50) DEFAULT NULL,
  `LastName` varchar(50) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Dumping data for table `student`
--

INSERT INTO `student` (`ID`, `FirstName`, `LastName`) VALUES
(10, 'Venus', 'Williams'),
(11, 'Fernando', 'Alonso'),
(12, 'David', 'Villa');

-- --------------------------------------------------------

--
-- Table structure for table `student_sport`
--

CREATE TABLE IF NOT EXISTS `student_sport` (
  `ID_Student` int(11) NOT NULL DEFAULT '0',
  `ID_Sport` int(11) NOT NULL DEFAULT '0'
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Dumping data for table `student_sport`
--

INSERT INTO `student_sport` (`ID_Student`, `ID_Sport`) VALUES
(10, 110),
(11, 111),
(12, 111),
(11, 112);

--
-- Indexes for dumped tables
--

--
-- Indexes for table `sport`
--
ALTER TABLE `sport`
 ADD PRIMARY KEY (`ID`);

--
-- Indexes for table `student`
--
ALTER TABLE `student`
 ADD PRIMARY KEY (`ID`);

--
-- Indexes for table `student_sport`
--
ALTER TABLE `student_sport`
 ADD PRIMARY KEY (`ID_Student`,`ID_Sport`), ADD KEY `ID_Sport` (`ID_Sport`);

--
-- Constraints for dumped tables
--

--
-- Constraints for table `student_sport`
--
ALTER TABLE `student_sport`
ADD CONSTRAINT `student_sport_ibfk_1` FOREIGN KEY (`ID_Student`) REFERENCES `student` (`ID`),
ADD CONSTRAINT `student_sport_ibfk_2` FOREIGN KEY (`ID_Sport`) REFERENCES `sport` (`ID`);

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
