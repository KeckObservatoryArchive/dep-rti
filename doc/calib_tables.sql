-- calibration association grouping tables
-- as more instruments are added, the nuber of cols will grow
-- set up for kcwi right now

CREATE TABLE IF NOT EXISTS koa_calib (
    koaid       VARCHAR(48)  NOT NULL,
    instrume    VARCHAR(15)  NOT NULL,
    koaimtyp    VARCHAR(25)  DEFAULT NULL,
    date_obs    DATE         DEFAULT NULL,
    stateid     VARCHAR(64)  DEFAULT NULL,
    last_mod    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (koaid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- each association between sciece and calib is a row
CREATE TABLE IF NOT EXISTS koa_calib_groups (
    science_koaid   VARCHAR(48)  NOT NULL,
    calib_koaid     VARCHAR(48)  NOT NULL,
    last_mod        TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (science_koaid, calib_koaid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
