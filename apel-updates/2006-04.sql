set autocommit=0;
DELETE FROM OSG_DATA WHERE Month = 04 AND Year = 2006 ;
INSERT INTO OSG_DATA VALUES ('Nebraska', 'cms', '8667', '13', '20', '8034', '12678', '04', '2006', '2006-04-01', '2006-04-30', '1.578', '2007-09-11 12:32:12');
INSERT INTO OSG_DATA VALUES ('Nebraska', 'usatlas', '12', '19', '29', '19', '30', '04', '2006', '2006-04-13', '2006-04-21', '1.578', '2007-09-11 12:32:12');
commit;
