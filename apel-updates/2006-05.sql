set autocommit=0;
DELETE FROM OSG_DATA WHERE Month = 05 AND Year = 2006 ;
INSERT INTO OSG_DATA VALUES ('Nebraska', 'cms', '16703', '191', '301', '10818', '17071', '05', '2006', '2006-05-01', '2006-05-31', '1.578', '2007-09-11 12:32:16');
INSERT INTO OSG_DATA VALUES ('Nebraska', 'usatlas', '3', '0', '0', '0', '0', '05', '2006', '2006-05-01', '2006-05-10', '1.578', '2007-09-11 12:32:16');
INSERT INTO OSG_DATA VALUES ('USCMS-FNAL-WC1-CE', 'cms', '29986', '46520', '60476', '77543', '100806', '05', '2006', '2006-05-02', '2006-05-31', '1.3', '2007-09-11 12:32:17');
INSERT INTO OSG_DATA VALUES ('USCMS-FNAL-WC1-CE', 'usatlas', '7', '172', '224', '216', '281', '05', '2006', '2006-05-08', '2006-05-29', '1.3', '2007-09-11 12:32:17');
commit;