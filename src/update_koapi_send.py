import datetime as dt
import db_conn


def update_koapi_send(utdate, semid, instr=None):
    """
    Looks for entry in koa.koapi_send with same semid and instr.
    If entry found that continues a consecutive date run, record is updated.
    Else a new record is inserted.
    Returns True/False
    """

    #print(f"updateKoapiSend: {utdate}, {semid}, {instr}")

    # db connect
    db = db_conn.db_conn('config.live.ini', configKey='DATABASE')

    #Get latest entry (by utdate_beg) matching semid and instr
    query = f"select * from koapi_send where semid='{semid}' "
    if instr: query += f" and instr='{instr}' " 
    query += " order by utdate_beg desc limit 1"
    rows = db.query('koa', query)
    #print(query, rows)

    #If no entry, then create one
    if (len(rows) == 0):

        #Make sure this is not a utdate past
        #note: allowing a 3 day buffer
        now = dt.datetime.now()
        ut = dt.datetime.strptime(utdate + ' 00:00:00' , '%Y-%m-%d %H:%M:%S')
        diff_sec = int((now - ut).total_seconds())
        diff_day = diff_sec / 86400

        if (diff_day > 3):
            #print("updateKoapiSend: New entry - first for semid, but in the past, so skipping.")
            pass
        else:
            #print("updateKoapiSend: New entry - first for semid")
            query = "insert into koapi_send set "
            query += f" semid='{semid}' "
            query += f", utdate_beg='{utdate}' "
            query += f", utdate_end='{utdate}' "
            query += f", send_data=1 "
            query += f", data_notified=0 "
            query += f", send_dvd=1 "
            query += f", dvd_notified=0 "
            if instr: query += f", instr='{instr}' "
            result = db.query('koa', query)

    # if existing entry see if we need to update (next day DEP only) or add a new one
    else:
        for row in rows:
            #print ("updateKoapiSend: DB Record found: " + row['semid']+" "+row['utdate_beg']+" "+row['utdate_end'])
            ut  = dt.datetime.strptime(utdate       + ' 00:00:00' , '%Y-%m-%d %H:%M:%S')
            end = dt.datetime.strptime(row['utdate_end'] + ' 00:00:00' , '%Y-%m-%d %H:%M:%S')
            diff_sec = int((ut - end).total_seconds())
            diff_day = int(diff_sec / 86400)

            if (diff_day == 0):
                #print("updateKoapiSend: Same day. No update required")
                break
            elif (diff_day == 1):
                #print("updateKoapiSend: Updating entry for semid")
                query = f"update koapi_send set utdate_end='{utdate}', send_data=1, send_dvd=1 "
                query += f" where semid='{semid}' and utdate_beg='{row['utdate_beg']}' "
                if instr: query += f" and instr='{instr}' " 
                result = db.query('koa', query)
                break
            else:
                if (diff_day < 0):
                    #print ("updateKoapiSend: In the past, no update required")
                    break
                else:
                    #print ("updateKoapiSend: New entry for semid")
                    query = "insert into koapi_send set "
                    query += f" semid='{semid}' "
                    query += f", utdate_beg='{utdate}' "
                    query += f", utdate_end='{utdate}' "
                    query += f", send_data=1 "
                    query += f", data_notified=0 "
                    query += f", send_dvd=1 "
                    query += f", dvd_notified=0 "
                    if instr: query += f", instr='{instr}' "
                    result = db.query('koa', query)
                    break

    return True

