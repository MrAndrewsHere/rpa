create database google_repo_db CHARACTER SET utf8 COLLATE utf8_general_ci;
/*
users rights
 */
grant file on *.* to goo@localhost;

/*
data clearing
*/

 CREATE TABLE `meet_logs_bulk_2703201430` (
  `date` varchar(100) DEFAULT NULL,
  `times` varchar(100) DEFAULT NULL,
  `event_name` varchar(300) DEFAULT NULL,
  `event_desc` varchar(300) DEFAULT NULL,
  `meet_code` varchar(200) DEFAULT NULL,
  `member_id` varchar(200) DEFAULT NULL,
  `external_member` varchar(200) DEFAULT NULL,
  `client_type` varchar(200) DEFAULT NULL,
  `org_email` varchar(200) DEFAULT NULL,
  `product_type` varchar(200) DEFAULT NULL,
  `duration` varchar(200) DEFAULT NULL,
  `member_name` varchar(200) DEFAULT NULL,
  `ipv4` varchar(200) DEFAULT NULL,
  `city` varchar(200) DEFAULT NULL,
  `country` varchar(200) DEFAULT NULL,
  `duration_sending_audio` varchar(200) DEFAULT NULL,
  `id_calendar` varchar(200) DEFAULT NULL,
  `id_meet` varchar(200) DEFAULT NULL,
  `duration_sending_video` varchar(200) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

 CREATE TABLE `meet_logs_bulk_full` (
  `datetimes` varchar(100) DEFAULT NULL,
  `event_name` varchar(300) DEFAULT NULL,
  `event_desc` varchar(300) DEFAULT NULL,
  `meet_code` varchar(200) DEFAULT NULL,
  `member_id` varchar(200) DEFAULT NULL,
  `external_member` varchar(200) DEFAULT NULL,
  `client_type` varchar(200) DEFAULT NULL,
  `org_email` varchar(200) DEFAULT NULL,
  `product_type` varchar(200) DEFAULT NULL,
  `duration` varchar(200) DEFAULT NULL,
  `member_name` varchar(200) DEFAULT NULL,
  `ipv4` varchar(200) DEFAULT NULL,
  `city` varchar(200) DEFAULT NULL,
  `country` varchar(200) DEFAULT NULL,
  `duration_sending_audio` varchar(200) DEFAULT NULL,
  `id_calendar` varchar(200) DEFAULT NULL,
  `id_meet` varchar(200) DEFAULT NULL,
  `duration_sending_video` varchar(200) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

load data infile '/var/tmp/meet_logs_1585894985535.csv' into table `meet_logs_bulk_fuul` FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\r\n';

CREATE TABLE `contingent_etalon` (
  `group` varchar(50) DEFAULT NULL,
  `direction` varchar(200) DEFAULT NULL,
  `department` varchar(200) DEFAULT NULL,
  `family` varchar(100) DEFAULT NULL,
  `name` varchar(100) DEFAULT NULL,
  `patron` varchar(100) DEFAULT NULL,
  `id1c` varchar(100) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

load data infile '/var/tmp/select_groups___groups_.csv' into table `contingent_etalon` FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\r\n';

alter table contingent_etalon add column `id` int(10) primary key auto_increment;
alter table contingent_etalon add column `date_add` date DEFAULT NULL;
update contingent_etalon set date_add=CURDATE();


 CREATE TABLE `meet_logs_cls` (
  `id` int(10) primary key auto_increment,
  `date` date DEFAULT NULL,
  `times` time DEFAULT NULL,
  `event_name` varchar(300) DEFAULT NULL,
  `event_desc` varchar(300) DEFAULT NULL,
  `meet_code` varchar(200) DEFAULT NULL,
  `member_id` varchar(200) DEFAULT NULL,
  `external_member` varchar(200) DEFAULT NULL,
  `client_type` varchar(200) DEFAULT NULL,
  `org_email` varchar(200) DEFAULT NULL,
  `product_type` varchar(200) DEFAULT NULL,
  `duration` int(10) DEFAULT NULL,
  `member_name` varchar(200) DEFAULT NULL,
  `ipv4` varchar(200) DEFAULT NULL,
  `city` varchar(200) DEFAULT NULL,
  `country` varchar(200) DEFAULT NULL,
  `duration_sending_audio` int(10) DEFAULT NULL,
  `id_calendar` varchar(200) DEFAULT NULL,
  `id_meet` varchar(200) DEFAULT NULL,
  `duration_sending_video` int(10) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*
Jan,Feb,Mar,Apr,May,Jun,Jul,Aug,Sep,Oct,Nov,Dec
*/
insert into `meet_logs_cls` (`date`,`times`,`event_name`,`event_desc`,`meet_code`,`member_id`,`external_member`,`client_type`,`org_email`,`product_type`, 
`duration`,`member_name`,`ipv4`,`city`,`country`,`duration_sending_audio`,`id_calendar`,`id_meet`,`duration_sending_video`) 
select  STR_TO_DATE(REPLACE(REPLACE(SUBSTRING_INDEX(ml.`datetimes`,',',1),'мар.','Mar'),' г.',''),'"%d %b %Y'),
        STR_TO_DATE(REPLACE(SUBSTRING_INDEX(ml.`datetimes`,',',-1),' GMT+3',''),'%H:%i:%S'),
ml.`event_name`,ml.`event_desc`,ml.`meet_code`,ml.`member_id`,ml.`external_member`,ml.`client_type`,ml.`org_email`,ml.`product_type`, 
IF(TRIM(ml.`duration`)='', 0, CONVERT(TRIM(REPLACE(ml.`duration`,'\r\n','')),INTEGER)), ml.`member_name`,ml.`ipv4`,ml.`city`,ml.`country`,
IF(TRIM(ml.`duration_sending_audio`)='',0, CONVERT(TRIM(ml.`duration_sending_audio`),INTEGER)), ml.`id_calendar`,ml.`id_meet`, 
IF(TRIM(REPLACE(REPLACE(ml.`duration_sending_video`,'\r',''),'\n',''))='',0,CONVERT(TRIM(REPLACE(REPLACE(ml.`duration_sending_video`,'\r',''),'\n','')),INTEGER)) from `meet_logs_bulk_2703201430` ml;


/*groups - emails*/
CREATE TABLE group_emails_bulk (
  `event_name` varchar (200) DEFAULT NULL ,
  `description` varchar (500) DEFAULT NULL ,
  `user` varchar (200) DEFAULT NULL ,
  `dateadd` varchar (100) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

load data infile '/var/tmp/groupsalt_logs_1586335104438.csv' into table `group_emails_bulk` FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\r\n';

CREATE TABLE group_emails_cls (
  `id` int(10) primary key auto_increment,
  `date` date DEFAULT NULL,
  `times` time DEFAULT NULL,
  `groupname` varchar (200) DEFAULT NULL,
  `username` varchar (200) DEFAULT NULL
)ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*
support tables
*/

CREATE TABLE `academic_pair` (
	`id` int(10) primary key auto_increment,
	`time_start` time DEFAULT NULL,
	`time_end` time DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

insert into `academic_pair`(`time_start`,`time_end`) values 
('09:00:00','10:30:00'),
('10:40:00','12:10:00'),
('12:50:00','14:20:00'),
('14:30:00','16:00:00'),
('16:10:00','17:40:00'),
('17:50:00','19:20:00');


/*
перегрузка в таблицах
*/

insert into meet_logs_cls (`date`,`times`,`event_name`,`event_desc`,`meet_code`,`member_id`,`external_member`,`client_type`,`org_email`,`product_type`, 
`duration`,`member_name`,`ipv4`,`city`,`country`,`duration_sending_audio`,`id_calendar`,`id_meet`,`duration_sending_video`) 
select `date`,`times`,`event_name`,`event_desc`,`meet_code`,`member_id`,`external_member`,`client_type`,`org_email`,`product_type`, 
`duration`,`member_name`,`ipv4`,`city`,`country`,`duration_sending_audio`,`id_calendar`,`id_meet`,`duration_sending_video` from meet_logs_cls2;

/*
select count(meet_code), event_name, time_start, time_end from meet_logs_cls mc, academic_pair ap where timediff(times,time_start)>'00:00:00' and timediff(times,time_end)<'00:00:00' and duration>2500 group by event_name,time_start, time_end;

select meet_code,count(member_name), time_start, time_end from meet_logs_cls mc, academic_pair ap 
where timediff(times,time_start)>'00:00:00' and timediff(times,time_end)<'00:00:00' and event_name='Трансляция просмотрена' 
group by meet_code,time_start, time_end order by time_start;

select distinct member_name, member_id, time_start, time_end from meet_logs_cls mc, academic_pair ap where timediff(times,time_start)>'00:00:00' and timediff(times,time_end)<'00:00:00' and meet_code='SNGSDJSDXW' order by time_start, time_end;

select meet_code, times, count(*) from meet_logs_cls where meet_code='RECAPCMEQK' and timediff(times,(select time_start from academic_pair where id=1))>'00:00:00' and timediff(times,(select time_end from academic_pair where id=1))<'00:00:00' group by meet_code, times;

select meet_code,time_start, time_end, member_id, sum(duration), member_name from meet_logs_cls mc, academic_pair ap 
where timediff(times,time_start)>'00:00:00' and timediff(times,time_end)<'00:00:00' and event_name='Конечная точка отключена.' 
group by meet_code, member_name, member_id,time_start, time_end 
order by meet_code, member_name, member_id,time_start, time_end;

select meet_code,time_start, time_end, sum(duration) from meet_logs_cls mc, academic_pair ap 
where timediff(times, time_start)>'00:00:00' and timediff(times, time_end)<'00:00:00' and event_name='Конечная точка отключена.' and meet_code='YRVIZVBPJI'
group by meet_code, time_start, time_end 
order by time_start, time_end;



select distinct meet_code from meet_logs_cls2 where event_name='Конечная точка отключена.' and duration>2500 and timediff(times,'08:50:00')>'+00:00:00';
select distinct meet_code from meet_logs_cls2 where event_name='Трансляция просмотрена';


*/

SET @date_day='2020-03-26';
/*
число занятий на текущий час (сессии более 15 минут)
*/
select count(meet_code) as cnt_pairs15 from (select meet_code, sum(duration) as sum_time from meet_logs_cls where `date`=@date_day and member_id<>org_email and event_name='Конечная точка отключена.' group by meet_code) a where sum_time>900;

/*
число занятий на текущий час (сессии более 40 минут)
*/
select count(meet_code) as cnt_pairs40 from (select meet_code, sum(duration) as sum_time from meet_logs_cls where `date`=@date_day and member_id<>org_email and event_name='Конечная точка отключена.' group by meet_code) a where sum_time>2400;

/*
Число лекций на текущий час (это похоже отпадет, т.к. все лекции уже идут по принципу семинаров).
*/
select count(meet_code) as cnt_transl from (select meet_code, count(*) as cnt from meet_logs_cls where `date`=@date_day and event_name='Трансляция просмотрена' group by meet_code) a;

/*
Количество студентов присуствовавших на занятиях на текущий час (с длительностью сессии 15 мин)
*/
select count(member_name) as cnt_sudents15 from (select member_name, sum(duration) as sum_time from meet_logs_cls where `date`=@date_day and member_id<>org_email and event_name='Конечная точка отключена.' group by member_name) a where sum_time>900;


/*
сводка по работе университета
*/
select * from
(select count(meet_code) as cnt_pairs15 from (select meet_code, sum(duration) as sum_time from meet_logs_cls where `date`=@date_day and member_id<>org_email group by meet_code) a where sum_time>900) a,
(select count(meet_code) as cnt_pairs40 from (select meet_code, sum(duration) as sum_time from meet_logs_cls where `date`=@date_day and member_id<>org_email group by meet_code) a where sum_time>2400) b,
(select count(member_name) as cnt_sudents15 from (select member_name, sum(duration) as sum_time from meet_logs_cls where `date`=@date_day and member_id<>org_email group by member_name) a where sum_time>900) c,
(select count(member_name) as cnt_sudents40 from (select member_name, sum(duration) as sum_time from meet_logs_cls where `date`=@date_day and member_id<>org_email group by member_name) a where sum_time>2400) d;

/*
посещаемость по дням
*/
select cnt_pairs40, cnt_sudents40, b.dd from
(select count(meet_code) as cnt_pairs40,dd from
(select meet_code, sum(duration) as sum_time, `date` as dd from meet_logs_cls where member_id<>org_email group by `date`, meet_code) a where sum_time>2400 group by dd) b
left join
(select count(member_name) as cnt_sudents40,dd from
(select member_name, sum(duration) as sum_time, `date` as dd from meet_logs_cls where member_id<>org_email group by `date`, member_name) a where sum_time>2400 group by dd) d
on b.dd=d.dd;

/*
посещаемость в процентах в среднем
 */
select avgs, cnt, ((avgs*100)/cnt) as p from
  (select avg(cnt_sudents40) as avgs from
    (select count(member_name) as cnt_sudents40,dd from
    (select member_name, sum(duration) as sum_time, `date` as dd from meet_logs_cls where member_id<>org_email group by `date`, member_name) a
    where sum_time>2400 and dd>='2020-03-23' and dd<'2020-03-28' group by dd) d
) av, (select count(*) as cnt from contingent_etalon) ct;


/*
Отчет для подачи заявлений на частичную компенсацию стоимости питания
обучающимся очной формы обучения программ высшего образования

ФИО студента, количество занятий которые он посетил, день, группа
*/
select `date`,member_name, groupname, count(*) as cnt INTO OUTFILE '/var/tmp/stud_group_date.csv' FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\' LINES TERMINATED BY '\n'  from 
(select m.`date`, m.member_name, g.groupname, m.meet_code, sum(m.duration) as ss from meet_logs_cls m left join group_emails_cls g 
on m.member_id=concat(g.username,'@uni-dubna.ru') group by m.meet_code, m.`date`, m.member_name, g.groupname) a 
group by `date`, member_name, groupname;





