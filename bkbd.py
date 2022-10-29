#!/usr/bin/env python
import subprocess, mysql.connector, os, datetime, zipfile, boto3
from boto3.s3.transfer import S3Transfer
from mysql.connector import errorcode


now = datetime.datetime.now()
now = now.strftime("%Y-%m-%d %H:%M:%S")


def connect(config):
    """
    Toma un diccionario de parámetros de conexión como entrada y devuelve un objeto de cursor si la
    conexión es exitosa

    :param config: un diccionario con las siguientes claves:
    :return: El objeto del cursor está siendo devuelto.
    """
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
        return cursor


def query_user(cursor):
    """
    Toma un objeto de cursor como argumento y devuelve una lista de todos los usuarios en la base de
    datos.

    :param cursor: El objeto cursor
    :return: Una lista de usuarios
    """
    try:
        cursor.execute("select user from mysql.user;")
        list_users = []
        for (user,) in cursor.fetchall():
            list_users.append(user.decode())
        return list_users
    except mysql.connector.Error as err:
        print(err)


def main(list_users, time):
    """
    Toma una lista de usuarios, y para cada usuario, crea un archivo de respaldo con el nombre del
    usuario y la fecha actual

    :param list_users: Esta es una lista de todos los usuarios en la base de datos
    """
    try:
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
                        ".sql",
                    ]
                )
                os.remove(userbk + now + ".sql")
    except Exception as e:
        print(e)


def compress():
    """
    Crea un archivo zip llamado backup.zip, luego recorre la carpeta de respaldo y agrega todos los
    archivos PDF al archivo zip.
    """
    try:
        backup = zipfile.ZipFile(".\backup.zip", "w")

        for folder, subfolders, files in os.walk(".\backup"):
            for file in files:
                if file.endswith(".pdf"):
                    backup.write(
                        os.path.join(folder, file),
                        os.path.relpath(os.path.join(folder, file), ".\backup"),
                        compress_type=zipfile.ZIP_DEFLATED,
                    )
        backup.close()
    except Exception as e:
        print(e)


def put_s3(aws_access_key_id, aws_secret_access_key, bucket, key):
    """
    Toma las credenciales de AWS y el nombre y la clave del depósito, y carga el archivo en S3

    :param aws_access_key_id: Su ID de clave de acceso de AWS
    :param aws_secret_access_key: La clave de acceso secreta para su cuenta de AWS
    :param bucket: El nombre del depósito al que desea cargar
    :param key: El nombre del archivo que desea cargar en S3
    :return: Se devuelve file_url.
    """
    try:
        credentials = {
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
        }

        client = boto3.client("s3", "us-west-2", **credentials)
        transfer = S3Transfer(client)
        transfer.upload_file("./backup", bucket, key, extra_args={"ACL": "public-read"})
        file_url = "%s/%s/%s" % (client.meta.endpoint_url, bucket, key)
        return file_url
    except Exception as e:
        print(e)


def eliminar_archivos():
    """
    Elimina el archivo backup.zip de la carpeta de copia de seguridad
    """
    try:
        os.remove(os.path.join("./backup", "backup.zip"))
    except Exception as e:
        print(e)


# Esto le pide al usuario que ingrese el host, el usuario y la contraseña.
host = input("Ingrese el host: ")
user = input("Ingrese el usuario: ")
password = input("Ingrese la contraseña: ")


# Creación de un diccionario con el host, el usuario y la contraseña.
config = {
    "host": host,
    "user": user,
    "password": password,
}


# Esto le pide al usuario que ingrese sus credenciales de AWS.
aws_access_key_id = input("Ingrese su aws_access_key_id: ")
aws_secret_access_key = input("Ingrese su aws_secret_access_key: ")
bucket = input("Ingrese el nombre del bucket: ")
key = input("Ingrese el nombre del archivo: ")


if __name__ == "__main__":
    main(query_user(connect(config)), now)
    compress()
    put_s3(aws_access_key_id, aws_secret_access_key, bucket, key)
