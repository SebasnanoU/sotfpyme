# Script Backup Schemas Usuarios

### Descripción: 
Se realiza un backup de los schemas de los usuarios de la base de datos.
La logica de este script es que se debe ejecutar en el servidor de base de datos, y se debe tener acceso a la base de datos de mysql.
se crea un archivo .sql por cada schema de usuario, y se almacena en la carpeta /backup que se comprime en un archivo .zip, el cual se almacena en la nube.
