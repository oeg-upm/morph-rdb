-- phpMyAdmin SQL Dump
-- version 4.2.7.1
-- http://www.phpmyadmin.net
--
-- Servidor: 127.0.0.1
-- Tiempo de generación: 17-01-2015 a las 16:36:02
-- Versión del servidor: 5.6.20
-- Versión de PHP: 5.5.15

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;

--
-- Base de datos: `d011b`
--

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `contact`
--

CREATE TABLE IF NOT EXISTS `contact` (
  `ID` int(11) NOT NULL,
  `SID` int(11) NOT NULL,
  `Email` varchar(50) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Volcado de datos para la tabla `contact`
--

INSERT INTO `contact` (`ID`, `SID`, `Email`) VALUES
(1, 10, 'venus@hotmail.com'),
(2, 10, 'venus@gmail.com'),
(3, 11, 'fernando@yahoo.com'),
(4, 12, 'david@msn.com');

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `person`
--

CREATE TABLE IF NOT EXISTS `person` (
  `ID` int(11) NOT NULL,
  `SSN` varchar(50) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Volcado de datos para la tabla `person`
--

INSERT INTO `person` (`ID`, `SSN`) VALUES
(10, '1234510'),
(11, '1234511'),
(12, '1234512');

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `sport`
--

CREATE TABLE IF NOT EXISTS `sport` (
  `ID` int(11) NOT NULL,
  `Description` varchar(50) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Volcado de datos para la tabla `sport`
--

INSERT INTO `sport` (`ID`, `Description`) VALUES
(110, 'Tennis'),
(111, 'Football'),
(112, 'Formula1');

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `student`
--

CREATE TABLE IF NOT EXISTS `student` (
  `ID` int(11) NOT NULL,
  `FirstName` varchar(50) DEFAULT NULL,
  `LastName` varchar(50) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Volcado de datos para la tabla `student`
--

INSERT INTO `student` (`ID`, `FirstName`, `LastName`) VALUES
(10, 'Venus', 'Williams'),
(11, 'Fernando', 'Alonso'),
(12, 'David', 'Villa');

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `student_sport`
--

CREATE TABLE IF NOT EXISTS `student_sport` (
  `ID_Student` int(11) NOT NULL DEFAULT '0',
  `ID_Sport` int(11) NOT NULL DEFAULT '0'
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Volcado de datos para la tabla `student_sport`
--

INSERT INTO `student_sport` (`ID_Student`, `ID_Sport`) VALUES
(10, 110),
(11, 111),
(12, 111),
(11, 112);

--
-- Índices para tablas volcadas
--

--
-- Indices de la tabla `contact`
--
ALTER TABLE `contact`
 ADD PRIMARY KEY (`ID`), ADD KEY `SID` (`SID`);

--
-- Indices de la tabla `person`
--
ALTER TABLE `person`
 ADD PRIMARY KEY (`ID`);

--
-- Indices de la tabla `sport`
--
ALTER TABLE `sport`
 ADD PRIMARY KEY (`ID`);

--
-- Indices de la tabla `student`
--
ALTER TABLE `student`
 ADD PRIMARY KEY (`ID`);

--
-- Indices de la tabla `student_sport`
--
ALTER TABLE `student_sport`
 ADD PRIMARY KEY (`ID_Student`,`ID_Sport`), ADD KEY `ID_Sport` (`ID_Sport`);

--
-- Restricciones para tablas volcadas
--

--
-- Filtros para la tabla `contact`
--
ALTER TABLE `contact`
ADD CONSTRAINT `contact_ibfk_1` FOREIGN KEY (`SID`) REFERENCES `student` (`ID`);

--
-- Filtros para la tabla `student`
--
ALTER TABLE `student`
ADD CONSTRAINT `student_ibfk_1` FOREIGN KEY (`ID`) REFERENCES `person` (`ID`);

--
-- Filtros para la tabla `student_sport`
--
ALTER TABLE `student_sport`
ADD CONSTRAINT `student_sport_ibfk_1` FOREIGN KEY (`ID_Student`) REFERENCES `student` (`ID`),
ADD CONSTRAINT `student_sport_ibfk_2` FOREIGN KEY (`ID_Sport`) REFERENCES `sport` (`ID`);

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
