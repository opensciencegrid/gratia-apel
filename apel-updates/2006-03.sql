set autocommit=0;
DELETE FROM OSG_DATA WHERE Month = 03 AND Year = 2006 ;
INSERT INTO OSG_DATA VALUES ('Nebraska', 'cms', '6031', '1145', '1807', '16151', '25486', '03', '2006', '2006-03-01', '2006-03-31', '1.578', '2007-09-12 13:00:37');
INSERT INTO OSG_DATA VALUES ('Nebraska', 'usatlas', '305', '0', '1', '6059', '9562', '03', '2006', '2006-03-04', '2006-03-07', '1.578', '2007-09-12 13:00:37');
commit;
