
class FdtLock:

    def __init__(self, conn, lock_name, log):
        self.conn = conn
        self.lock_name = lock_name
        self.log = log

    def acquire(self):
        """
        Acquire the lock.
        """
        self.conn.ensure_connected()

        with self.conn.db.cursor() as cur:
            cur.execute("SELECT GET_LOCK(%s, 0) AS acquired", (self.lock_name,))
            acquired = cur.fetchone()["acquired"]

            cur.execute("SELECT CONNECTION_ID() AS id")
            conn_id = cur.fetchone()["id"]

            cur.execute("SELECT IS_USED_LOCK(%s) AS owner", (self.lock_name,))
            owner = cur.fetchone()["owner"]

        self.log.info(f"GET_LOCK={acquired}, connection={conn_id}, owner={owner}")

        return acquired == 1

    def check(self):
        """
        Check if the lock is still connected.

        If not,  try once to re-acquire the lock.
        """
        with self.conn.db.cursor() as cur:
            cur.execute("SELECT IS_USED_LOCK(%s) AS owner", (self.lock_name,))
            owner = cur.fetchone()["owner"]

            cur.execute("SELECT CONNECTION_ID() AS id")
            my_id = cur.fetchone()["id"]

        if owner == my_id:
            return True, my_id

        retry_sucess = self.acquire()
        if retry_sucess:
            return True, my_id

        return False, my_id


    def release(self):
        """
        Release the lock.
        """
        if self.conn.db is None:
            return

        try:
            with self.conn.db.cursor() as cur:
                cur.execute("SELECT RELEASE_LOCK(%s) AS released", (self.lock_name,))

                released = cur.fetchone()["released"]

                if released == 1:
                    self.log.info(f"Released lock '{self.lock_name}'.")
                elif released == 0:
                    self.log.warning(f"Lock {self.lock_name} is not held.")
                else:
                    self.log.warning(f"Lock {self.lock_name} does not exist.")

        except Exception:
            self.log.exception("Failed to release lock")

        finally:
            try:
                self.conn.db.close()
                self.conn.db = None
            except Exception:
                pass