#!/usr/bin/env python
import subprocess, mysql.connector, os, datetime, json, zipfile, boto3
from boto3.s3.transfer import S3Transfer
from mysql.connector import errorcode


def compress():
    backup = zipfile.ZipFile('.\backup.zip', 'w')
    
    for folder, subfolders, files in os.walk('.\backup'):
        for file in files:
            if file.endswith('.pdf'):
                backup.write(os.path.join(folder, file), os.path.relpath(os.path.join(folder,file), '.\backup'), compress_type = zipfile.ZIP_DEFLATED)
    backup.close()


def put_object(self, s3_client):
    try:
        with open(self.file, 'rb') as fd:
            response = s3_client.put_object(
                Bucket=self.bucket,
                Key=self.key,
                Body=fd
            )
        print(json.dumps(response, sort_keys=True, indent=4))
        print("Put Object exitoso")
        return True
    except FileNotFoundError:
        print("Archivo no encontrado")
        return False
    except Exception as e:
        print(str(e))
        return False


host = input("Ingrese el host: ")
user = input("Ingrese el usuario: ")
password = input("Ingrese la contraseña: ")


# Obtain connection string information from the portal
config = {
    "host": host,
    "user": user,
    "password": password,
}

# Construct connection string

try:
    conn = mysql.connector.connect(**config)
    print("Connection established")
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with the user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)
else:
    cursor = conn.cursor()


now = datetime.datetime.now()
now = now.strftime("%Y-%m-%d %H:%M:%S")
cursor.execute("select user from mysql.user;")
list_users = []
for (user,) in cursor.fetchall():
    list_users.append(user.decode())

for userbk in list_users:
    if (
        userbk != "azure_superuser"
        and userbk != "mysql.session"
        and userbk != "mysql.sys"
        and userbk != "root"
    ):
        subprocess.call(
            [
                "mysqldump",
                "-Fc",
                "-v",
                "-h softpyme.mysql.database.azure.com",
                "-u sebas",
                "-p JUan1030667249",
                userbk,
                "-r",
                "./backup/" + userbk + ".sql",
                ">",
                userbk,
                now,
                ".sql"
            ]
        )
        os.remove(userbk + now + ".sql")


aws_access_key_id = input("Ingrese su aws_access_key_id: ")
aws_secret_access_key = input("Ingrese su aws_secret_access_key: ")
bucket = input("Ingrese el nombre del bucket: ")
key = input("Ingrese el nombre del archivo: ")


credentials = { 
    'aws_access_key_id': aws_access_key_id,
    'aws_secret_access_key': aws_secret_access_key
}

client = boto3.client('s3', 'us-west-2', **credentials)
transfer = S3Transfer(client)

transfer.upload_file('/tmp/myfile', bucket, key,
                     extra_args={'ACL': 'public-read'})

file_url = '%s/%s/%s' % (client.meta.endpoint_url, bucket, key)

if __name__ == '__main__':
    compress()
    put_object(file_url, client)