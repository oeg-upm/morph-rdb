-- phpMyAdmin SQL Dump
-- version 4.0.4.1
-- http://www.phpmyadmin.net
--
-- Servidor: localhost
-- Tiempo de generación: 05-02-2014 a las 11:24:56
-- Versión del servidor: 5.6.12
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
-- Estructura de tabla para la tabla `Sport`
--

CREATE TABLE IF NOT EXISTS `Sport` (
  `id` int(11) NOT NULL DEFAULT '0',
  `name` varchar(50) DEFAULT NULL,
  `code` char(8) DEFAULT NULL,
  `type` char(8) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Volcado de datos para la tabla `Sport`
--

INSERT INTO `Sport` (`id`, `name`, `code`, `type`) VALUES
(100, 'Tennis', 'TNS', 'BOTH'),
(200, 'Chess', 'CHS', 'INDOOR'),
(300, 'Soccer', 'SCR', 'OUTDOOR');

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `Student`
--

CREATE TABLE IF NOT EXISTS `Student` (
  `id` char(8) NOT NULL DEFAULT '0',
  `name` varchar(50) DEFAULT NULL,
  `sport` int(11) DEFAULT NULL,
  `status` varchar(10) NOT NULL,
  `webpage` varchar(100) DEFAULT NULL,
  `phone` varchar(100) DEFAULT NULL,
  `email` varchar(100) DEFAULT NULL,
  `suffix` varchar(8) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `Sport` (`sport`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Volcado de datos para la tabla `Student`
--

INSERT INTO `Student` (`id`, `name`, `sport`, `status`, `webpage`, `phone`, `email`, `suffix`) VALUES
('B1', 'Paul', 100, 'active', NULL, '777-3426', NULL, 'Jr.'),
('B2', 'John', 200, 'active', NULL, NULL, 'john@acd.edu', 'Sr.'),
('B3', 'George', 300, 'active', 'www.george.edu', NULL, NULL, 'Sr. '),
('B4', 'Ringo', NULL, 'active', 'www.starr.edu', '888-4537', 'ringo@acd.edu', 'Jr. ');

--
-- Restricciones para tablas volcadas
--

--
-- Filtros para la tabla `Student`
--
ALTER TABLE `Student`
  ADD CONSTRAINT `Student_ibfk_1` FOREIGN KEY (`sport`) REFERENCES `Sport` (`id`);

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
