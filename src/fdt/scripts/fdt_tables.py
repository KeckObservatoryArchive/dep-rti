"""
Used to create the KOA database FDT tables

/usr/local/anaconda/bin/python3 fdt_tables.py
"""
from fdt.fdt_database import DatabaseConnect
from fdt.fdt_utils import read_config

class Tables:
    """
    tables = Tables()

    cursor.execute(tables.fdt_packages())
    cursor.execute(tables.fdt_observations())
    """

    def fdt_packages(self):
        """
        status = CLOSE_REQUESTED is for CLI
        """
        return """
        CREATE TABLE fdt_packages (
            pkg_id          BIGINT AUTO_INCREMENT PRIMARY KEY,
            run_number      BIGINT NOT NULL DEFAULT 1,
            filename        VARCHAR(255) NOT NULL,
            filepath        VARCHAR(255) NOT NULL,
            instrument      VARCHAR(255) NOT NULL,
            level           INT NOT NULL,
            status          ENUM('OPEN','CLOSED','TRANSFERRING', 
                                 'COMPLETE','ERROR', 'CLOSE_REQUESTED',
                                 'IGNORE') NOT NULL,
            xfr_pid         BIGINT,
            creation_time   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            closed_time     DATETIME NULL,
            xfr_start_time  DATETIME NULL,
            xfr_end_time    DATETIME NULL,
            filesize_mb     DOUBLE NOT NULL DEFAULT 0,
            koaid_count       INT NOT NULL DEFAULT 0,
            source_deleted  TINYINT(1) NOT NULL DEFAULT 0,
            last_mod        DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                            ON UPDATE CURRENT_TIMESTAMP
        );
        """

    def fdt_observations(self):
        return """
        CREATE TABLE fdt_observations (
            obsid             BIGINT AUTO_INCREMENT PRIMARY KEY,
            koaid             VARCHAR(48) NOT NULL,
            instrument      VARCHAR(255) NOT NULL,
            level             INT NOT NULL,
            filepath          VARCHAR(255) NOT NULL,
            filepath_replacement VARCHAR(255) NULL,
            inserted_time    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            pkg_id           BIGINT,
            pkg_start_time   DATETIME NULL,
            pkg_end_time     DATETIME NULL,
            status            ENUM('PENDING','PACKAGING','PACKAGED','IGNORE',
                                   'TRANSFERRING','COMPLETE','ERROR') NOT NULL,
            last_mod          DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                              ON UPDATE CURRENT_TIMESTAMP,
                              
            UNIQUE KEY unique_koaid_level (koaid, level)
        );
        """


def main():


    tables = Tables()

    cfg_file = f'fdt/fdt_config.live.yaml'

    cfg = read_config(cfg_file)

    conn_obj = DatabaseConnect(cfg["DATABASE"])
    conn_obj.connect()

    try:
        with conn_obj.db.cursor() as cursor:

            # Drop tables
            cursor.execute("DROP TABLE IF EXISTS fdt_observations")
            cursor.execute("DROP TABLE IF EXISTS fdt_packages")

            # Create tables
            cursor.execute(tables.fdt_packages())
            cursor.execute(tables.fdt_observations())

        print("Created fdt_packages and fdt_observations.")

    finally:
        conn_obj.db.close()


if __name__ == "__main__":
    main()
