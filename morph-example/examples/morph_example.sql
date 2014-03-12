-- phpMyAdmin SQL Dump
-- version 4.0.4.1
-- http://www.phpmyadmin.net
--
-- Servidor: 127.0.0.1
-- Tiempo de generación: 12-03-2014 a las 19:10:10
-- Versión del servidor: 5.6.11
-- Versión de PHP: 5.5.3

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;

--
-- Base de datos: `morph_example`
--
CREATE DATABASE IF NOT EXISTS `morph_example` DEFAULT CHARACTER SET latin1 COLLATE latin1_swedish_ci;
USE `morph_example`;

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `sport`
--

CREATE TABLE IF NOT EXISTS `sport` (
  `id` int(11) NOT NULL DEFAULT '0',
  `name` varchar(50) DEFAULT NULL,
  `code` char(8) NOT NULL,
  `type` char(8) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Volcado de datos para la tabla `sport`
--

INSERT INTO `sport` (`id`, `name`, `code`, `type`) VALUES
(100, 'Tennis', 'TNS', 'BOTH'),
(200, 'Chess', 'CHS', 'INDOOR'),
(300, 'Soccer', 'SCR', 'OUTDOOR');

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `student`
--

CREATE TABLE IF NOT EXISTS `student` (
  `id` char(8) NOT NULL DEFAULT '0',
  `name` varchar(50) DEFAULT NULL,
  `sport` int(11) DEFAULT NULL,
  `status` varchar(10) NOT NULL,
  `webpage` varchar(100) DEFAULT NULL,
  `phone` varchar(100) DEFAULT NULL,
  `email` varchar(100) DEFAULT NULL,
  `suffix` varchar(8) DEFAULT NULL,
  `birthdate` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `Sport` (`sport`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Volcado de datos para la tabla `student`
--

INSERT INTO `student` (`id`, `name`, `sport`, `status`, `webpage`, `phone`, `email`, `suffix`, `birthdate`) VALUES
('B1', 'Paul', 100, 'active', NULL, '777-3426', NULL, 'Jr.', '2000-12-31 00:00:00'),
('B2', 'John', 200, 'active', NULL, NULL, 'john@acd.edu', 'Sr.', NULL),
('B3', 'George', 300, 'active', 'www.george.edu', NULL, NULL, 'Sr. ', '1990-06-18 00:00:00'),
('B4', 'Ringo', NULL, 'active', 'www.starr.edu', '888-4537', 'ringo@acd.edu', 'Jr. ', NULL);

--
-- Restricciones para tablas volcadas
--

--
-- Filtros para la tabla `student`
--
ALTER TABLE `student`
  ADD CONSTRAINT `Student_ibfk_1` FOREIGN KEY (`sport`) REFERENCES `sport` (`id`);

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
