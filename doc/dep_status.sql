CREATE TABLE IF NOT EXISTS `dep_status` (
  `id`            int(11)       NOT NULL        AUTO_INCREMENT PRIMARY KEY,      
  `instr`         varchar(16)   NOT NULL                        COMMENT 'Instrument name',      
  `filepath`      text          NOT NULL                        COMMENT 'Original file input location',      
  `koaid`         varchar(24)   DEFAULT NULL    UNIQUE          COMMENT 'Unique KOAID',     
  `utc_datetime`  datetime      DEFAULT NULL                    COMMENT 'Datetime of image (utdate + utc)',      
  `raw_savepath`  text          DEFAULT NULL                    COMMENT 'Local archived raw file location',     
  `arch_stat`     varchar(16)   DEFAULT NULL                    COMMENT 'Overall archive status [NULL, QUEUED, PROCESSING, INVALID, DONE, ERROR]',  
  `file_valid`    varchar(32)   DEFAULT NULL                    COMMENT '[YES, EMPTY, UNREADABLE, DUPLICATE, etc]',   
  `file_size`     double        DEFAULT NULL                    COMMENT 'FITS size in MB',  
  `arch_size`     double        DEFAULT NULL                    COMMENT 'Archive size (including ancillary files)',  
  `sdata_dir`     varchar(8)    DEFAULT NULL                    COMMENT 'Numerical sdata dir suffix (ie 904, 401)',  
  `creation_time` datetime      DEFAULT NULL                    COMMENT 'Time record created',      
  `arch_time`     datetime      DEFAULT NULL                    COMMENT 'Time file ready for transfer',  
  `xfr_time`      datetime      DEFAULT NULL                    COMMENT 'Time files tranferred to IPAC', 
  `tpx_time`      datetime      DEFAULT NULL                    COMMENT 'Time of IPAC response', 
  `tpx_stat`      varchar(32)   DEFAULT NULL                    COMMENT 'IPAC response [ERROR, etc]', 
  `image_type`    varchar(32)   DEFAULT NULL                    COMMENT '[Science, calibration, etc]',   
  `semid`         varchar(10)   DEFAULT NULL                    COMMENT 'Semester + ProgID',   
  `header_json`   json          DEFAULT NULL                    COMMENT 'Store all FITS header info as json',    
  `last_mod`      timestamp     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ;


CREATE TABLE IF NOT EXISTS `dep_error` (
  `id`            int(11)       NOT NULL        AUTO_INCREMENT PRIMARY KEY,      
  `instr`         varchar(16)   DEFAULT NULL                    COMMENT 'Instrument name',      
  `code`          varchar(32)   DEFAULT NULL                    COMMENT 'Error code string',   
  `info`          text          DEFAULT NULL                    COMMENT 'Text description of error',      
  `script`        varchar(256)  DEFAULT NULL                    COMMENT 'Script reporting the error',  
  `status`        varchar(10)   DEFAULT NULL                    COMMENT '[RESOLVED, DELETED]',   
  `creation_time` datetime      NOT NULL DEFAULT CURRENT_TIMESTAMP,      
  `last_mod`      timestamp     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ;