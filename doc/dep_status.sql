
CREATE TABLE IF NOT EXISTS `dep_status` (
  `koaid`               varchar(30)   PRIMARY KEY   COMMENT 'Unique KOA ID',
  `instrument`          varchar(15)   NOT NULL      COMMENT 'Instrument name',
  `utdatetime`          datetime      NOT NULL      COMMENT 'DATE-OBS UTC',
  `status`              varchar(15)                 COMMENT 'Current status of archive process [QUEUED, PROCESSING, COMPLETE, INVALID, ERROR]',
  `status_code`         varchar(25)                 COMMENT 'Status code of archive process [NULL, DUPLICATE, EMPTY, UNREADABLE, etc]',
  `ofname`              varchar(255)                COMMENT 'Full path to original file (sdata location)',
  `stage_file`          varchar(255)                COMMENT 'Full path the staged original raw file',
  `archive_dir`         varchar(255)                COMMENT 'Directory file is archived',
  `creation_time`       datetime                    COMMENT 'Date and time the FITS file is ready to be processed',
  `dep_start_time`      datetime                    COMMENT 'Date and time that DEP processing started',
  `dep_end_time`        datetime                    COMMENT 'Date and time that file processing is complete',
  `xfr_start_time`      datetime                    COMMENT 'Date and time that transfer started',
  `xfr_end_time`        datetime                    COMMENT 'Date and time that transfer is complete',
  `ipac_notify_time`    datetime                    COMMENT 'Date and time that IPAC is notified to start ingestion',
  `ipac_response_time`  datetime                    COMMENT 'Date and time that IPAC ingestion response received',
  `stage_time`          datetime                    COMMENT 'Date and time that original file copied to stage directory',
  `filesize_mb`         double                      COMMENT 'FITS file size in megabytes',
  `archsize_mb`         double                      COMMENT 'Size of complete FITS dataset in megabytes',
  `koaimtyp`            varchar(20)                 COMMENT 'Image type of the FITS file',
  `semid`               varchar(15)                 COMMENT 'SEMID of FITS file association',
  `last_mod`            timestamp     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ;


CREATE TABLE `dep_status_history` like `dep_status`;
alter table dep_status_history modify column `koaid` varchar(30) COMMENT 'KOA ID';
alter table dep_status_history drop primary key FIRST;
alter table dep_status_history add column `id`  int(11)  NOT NULL  AUTO_INCREMENT PRIMARY KEY before `koaid`;


CREATE TABLE IF NOT EXISTS `headers` (
  `koaid`         varchar(30)   PRIMARY KEY         COMMENT 'Unique KOA ID',
  `header`        json          DEFAULT NULL        COMMENT 'Store all FITS header info as json',    
  `last_mod`      timestamp     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ;
