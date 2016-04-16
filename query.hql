# Creation of tables from external sources

DROP TABLE ${hiveTableName1};
CREATE EXTERNAL TABLE ${hiveTableName1}(productcolorid int, genderlabel string, suppliercolorlabel string, seasonlabel string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\073'
STORED AS TEXTFILE LOCATION '${hiveDataFolder1}';

DROP TABLE ${hiveTableName2};
CREATE EXTERNAL TABLE ${hiveTableName2}(customerid int, domaincode string, birthdate timestamp, gender string, size int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\073'
STORED AS TEXTFILE LOCATION '${hiveDataFolder2}';

DROP TABLE ${hiveTableName3};
CREATE EXTERNAL TABLE ${hiveTableName3}(ordernum int, variantid int, customerid int, quantity int, unitprice int, orderdate timestamp)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\073'
STORED AS TEXTFILE LOCATION '${hiveDataFolder3}';

DROP TABLE ${hiveTableName4};
CREATE EXTERNAL TABLE ${hiveTableName4}(variantid int, productcolorid int, productid int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\073'
STORED AS TEXTFILE LOCATION '${hiveDataFolder4}';

DROP TABLE ${hiveTableName5};
CREATE EXTERNAL TABLE ${hiveTableName5}(variantid int, minsize int, maxsize int, size string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\073'
STORED AS TEXTFILE LOCATION '${hiveDataFolder5}';

# Intermediate Aggregations

DROP TABLE temp;
CREATE TABLE temp AS SELECT CUSTOMERID, GENDERLABEL,count(*) AS COUNT FROM 
(SELECT CUSTOMERID, ORDERNUM, VARIANTID, QUANTITY, UNITPRICE 
FROM ${hiveTableName3})x 
JOIN (SELECT p.VARIANTID, p.PRODUCTCOLORID, c.GENDERLABEL, c.SEASONLABEL 
FROM ${hiveTableName4} p JOIN ${hiveTableName1} c
ON (p.PRODUCTCOLORID = c.PRODUCTCOLORID))y
ON (x.VARIANTID = y.VARIANTID)
GROUP BY CUSTOMERID,GENDERLABEL;

DROP TABLE temp2;
create table temp2 as select CUSTOMERID, DOMINANT_GENDER,
CASE DOMINANT_GENDER
	WHEN 'Femme' THEN 1
	WHEN 'Homme' THEN 2
	WHEN 'Enfant' THEN 3
	WHEN 'Sacs' THEN 4
	WHEN 'Accessoires' THEN 5
END AS RANK
FROM
(select t.CUSTOMERID,t.GENDERLABEL as DOMINANT_GENDER from temp t
join (select CUSTOMERID,max(count) as MAX from temp group by CUSTOMERID)x
on (t.CUSTOMERID = x.CUSTOMERID AND t.COUNT = x.MAX))a;

DROP TABLE temp1;
CREATE TABLE temp1 AS SELECT CUSTOMERID, SIZE,count(*) AS COUNT FROM 
(SELECT CUSTOMERID, ORDERNUM, VARIANTID, QUANTITY, UNITPRICE 
FROM ${hiveTableName3})x
JOIN (SELECT VARIANTID, SIZE 
FROM ${hiveTableName5})y
ON (x.VARIANTID = y.VARIANTID)
GROUP BY CUSTOMERID,SIZE;

# Resultant Table for Customer Aggregations

INSERT OVERWRITE DIRECTORY '${hiveDataFolder6}' 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
select m.*,n.LAST_PURCHASE_DATE from
(select a.CUSTOMERID, a.DOMINANT_GENDER, b.DOMINANT_SIZE from
(select t.CUSTOMERID, t.DOMINANT_GENDER from temp2 t
join (select CUSTOMERID,min(RANK) as RANK from temp2 group by CUSTOMERID)x
on (t.CUSTOMERID = x.CUSTOMERID AND t.RANK = x.RANK))a
join
(select t.CUSTOMERID,max(t.SIZE) as DOMINANT_SIZE from temp1 t
join (select CUSTOMERID,max(count) as MAX from temp1 group by CUSTOMERID)x
on (t.CUSTOMERID = x.CUSTOMERID AND t.COUNT = x.MAX)
group by t.CUSTOMERID)b
on(a.CUSTOMERID = b.CUSTOMERID))m
join
(select CUSTOMERID, max(ORDERDATE) AS LAST_PURCHASE_DATE from ${hiveTableName3} group by CUSTOMERID)n
on(m.CUSTOMERID = n.CUSTOMERID);

# Resultant Table for Product Aggregations

INSERT OVERWRITE DIRECTORY '${hiveDataFolder7}' 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
select m.*,n.distinct_customers from
(select productid, sum(unitprice) as total_amount,sum(quantity) as total_quantity
from (select p.productid,p.variantid,o.unitprice,o.quantity from ${hiveTableName4} p
join (select variantid,unitprice,quantity from ${hiveTableName3})o
on(p.variantid = o.variantid))z
group by productid
order by total_amount desc)m
join 
(select productid,count(customerid) as distinct_customers from
(select distinct * from (select p.productid,o.customerid from ${hiveTableName4} p
join (select customerid,variantid,unitprice,quantity from ${hiveTableName3})o
on(p.variantid = o.variantid))y)z
group by productid)n
on(m.productid = n.productid);
