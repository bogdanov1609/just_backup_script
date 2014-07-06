#!/usr/bin/python
# -*- coding: utf-8 -*-


import ConfigParser
import MySQLdb
import os
import datetime
import tarfile
import ftplib


__author__ = 'spooner'


def mysql_connect(auth_data):
    try:
        db = MySQLdb.connect(host=auth_data['mysql_host'], user=auth_data['mysql_username'], passwd=auth_data['mysql_password'])
        cursor = db.cursor()
    except MySQLdb.Error:
        print db.error()
    return cursor


def get_auth_data():
    config = ConfigParser.ConfigParser()
    config.read('auth.ini')
    auth_data = {}
    auth_data['mysql_username'] = config.get('mysql_auth', 'username')
    auth_data['mysql_password'] = config.get('mysql_auth', 'password')
    auth_data['mysql_host'] = config.get('mysql_auth', 'host')
    auth_data['ftp_username'] = config.get('ftp_auth', 'username')
    auth_data['ftp_password'] = config.get('ftp_auth', 'password')
    auth_data['ftp_host'] = config.get('ftp_auth', 'host')
    auth_data['backup_dir'] = config.get('options', 'directory')
    auth_data['upload'] = config.get('options', 'upload')
    return auth_data


def mysql_dump(auth_data):
    now_date = str(datetime.date.today())
    cursor = mysql_connect(auth_data)
    cursor.execute('show databases;')
    db_list = cursor.fetchall()

    mysql_backup_dir = auth_data['backup_dir'] + now_date + '/' + "mysql" + '/'

    if not os.path.exists(mysql_backup_dir):
        os.makedirs(mysql_backup_dir)

    for db in db_list:
        dump_cmd = "mysqldump -u " + str(auth_data['mysql_username']) + " -p" + str(auth_data['mysql_password']) + \
                   " -h " + str(auth_data['mysql_host']) + " " + str(db[0]) + " > " + mysql_backup_dir \
                    + str(db[0]) + ".sql 2> /dev/null &"
        os.system(dump_cmd)


def files_dump(auth_data):
    now_date = str(datetime.date.today())
    cursor = mysql_connect(auth_data)
    cursor.execute('select userid, homedir from ftpd.ftpuser')
    user_list = cursor.fetchall()
    ftp_backup_dir = auth_data['backup_dir'] + now_date + '/' + 'files' + '/'

    if not os.path.exists(ftp_backup_dir):
        os.makedirs(ftp_backup_dir)

    for i in user_list:
        tar = tarfile.open(ftp_backup_dir + i[0] + ".tar.gz", "w:gz")
        tar.add(i[1])
        tar.close()


def tar_all_backup(auth_data):
        now_date = str(datetime.date.today())
        today_backup = auth_data['backup_dir'] + now_date + ".tar.gz"
        tar = tarfile.open(today_backup, "w:gz")
        tar.add(auth_data['backup_dir'])
        return today_backup


def upload_to_ftp(auth_data, today_backup):
    session = ftplib.FTP(auth_data['ftp_host'], auth_data['ftp_username'], auth_data['ftp_password'])
    filename = open(today_backup, 'rb')
    session.storbinary('STOR ' + today_backup, filename)
    filename.close()
    session.quit()


def main():
    auth_data = get_auth_data()
    files_dump(auth_data)
    mysql_dump(auth_data)
    if auth_data['upload'] == 'true':
        today_backup = tar_all_backup(auth_data)
        upload_to_ftp(auth_data, today_backup)



if __name__ == "__main__":
    main()
