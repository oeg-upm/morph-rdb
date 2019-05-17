--
-- PostgreSQL database dump
--

-- Dumped from database version 9.4.3
-- Dumped by pg_dump version 9.4.3
-- Started on 2015-06-18 15:56:37

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

--
-- TOC entry 174 (class 3079 OID 11855)
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- TOC entry 2006 (class 0 OID 0)
-- Dependencies: 174
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


SET search_path = public, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- TOC entry 172 (class 1259 OID 16567)
-- Name: Sport; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE "Sport" (
    "ID" integer NOT NULL,
    "Name" character varying(50),
    "Code" character(8),
    "Type" character(8)
);


ALTER TABLE "Sport" OWNER TO postgres;

--
-- TOC entry 173 (class 1259 OID 16572)
-- Name: Student; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE "Student" (
    "ID" character(8) NOT NULL,
    "Name" character varying(50),
    "Sport" integer,
    "Status" character varying(50),
    "Webpage" character varying(50),
    "Phone" character varying(50),
    "Email" character varying(50),
    "Suffix" character varying(50),
    "Birthdate" date
);


ALTER TABLE "Student" OWNER TO postgres;

--
-- TOC entry 1997 (class 0 OID 16567)
-- Dependencies: 172
-- Data for Name: Sport; Type: TABLE DATA; Schema: public; Owner: postgres
--

INSERT INTO "Sport" ("ID", "Name", "Code", "Type") VALUES
(100, 'Tennis', 'TNS', 'BOTH'),
(200, 'Chess', 'CHS', 'INDOOR'),
(300, 'Soccer', 'SCR', 'OUTDOOR');

--
-- TOC entry 1998 (class 0 OID 16572)
-- Dependencies: 173
-- Data for Name: Student; Type: TABLE DATA; Schema: public; Owner: postgres
--



INSERT INTO "Student" ("ID", "Name", "Sport", "Status", "Webpage", "Phone", "Email", "Suffix", "Birthdate") VALUES
('B1', 'Paul', 100, 'active', NULL, '777-3426', NULL, 'Jr.', '2000-12-31 00:00:00'),
('B2', 'John', 200, 'active', NULL, NULL, 'john@acd.edu', 'Sr.', NULL),
('B3', 'George', 300, 'active', 'www.george.edu', NULL, NULL, 'Sr. ', '1990-06-18 00:00:00'),
('B4', 'Ringo', NULL, 'active', 'www.starr.edu', '888-4537', 'ringo@acd.edu', 'Jr. ', NULL);

--
-- TOC entry 1884 (class 2606 OID 16571)
-- Name: Sport_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres; Tablespace: 
--

ALTER TABLE ONLY "Sport"
    ADD CONSTRAINT "Sport_pkey" PRIMARY KEY ("ID");


--
-- TOC entry 1886 (class 2606 OID 16576)
-- Name: Student_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres; Tablespace: 
--

ALTER TABLE ONLY "Student"
    ADD CONSTRAINT "Student_pkey" PRIMARY KEY ("ID");


--
-- TOC entry 1887 (class 2606 OID 16577)
-- Name: Student_Sport_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY "Student"
    ADD CONSTRAINT "Student_Sport_fkey" FOREIGN KEY ("Sport") REFERENCES "Sport"("ID");


--
-- TOC entry 2005 (class 0 OID 0)
-- Dependencies: 5
-- Name: public; Type: ACL; Schema: -; Owner: postgres
--

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM postgres;
GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO PUBLIC;


-- Completed on 2015-06-18 15:56:37

--
-- PostgreSQL database dump complete
--

