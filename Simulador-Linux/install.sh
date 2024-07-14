#!/bin/bash



echo "Deseja instalar o mysql ?"
read -p "Digite 'y' para instalar o MySQL Server 8.0 (ou pressione Enter para pular): " install_mysql

if [[ "$install_mysql" == "y" ]]; then
    sudo apt update
    sudo apt remove -y  mysql*
    sudo apt install -y mysql-server-8.0
fi

 
sudo mysql_secure_installation
sudo  mysql -u root -e "ALTER USER 'root'@'localhost' IDENTIFIED WITH  mysql_native_password BY 'B#12905629#g';FLUSH PRIVILEGES;"

echo "\n\n\n"
echo "Deseja criar o banco com as tabelas do simulador (Todas as Simulações anteriores  serão apagadas) ?"
read -p "Digite 'y' para criar as tabelas do banco novamente (ou Enter para pular) : " create_database


if [[ "$create_database" == "y"  ]]; then

	echo "Apagando tabelas anteriores..."
	sudo mysql -uroot -pB#12905629#g  -e  "DROP DATABASE simuladorFinal;"
	echo "Criando o novo banco e suas tabelas..."
	sudo mysql -uroot -pB#12905629#g  -e  "CREATE DATABASE simuladorFinal;"
	sudo mysql -uroot -pB#12905629#g simuladorFinal < script.sql
fi 

echo "Instalando o ambiente python"

sudo apt update

sudo apt install -y  pip

pip install virtualenv

python3 -m virtualenv venv

source venv/bin/activate

pip install -r requirements.txt

echo "Ambiente Instalado com sucesso"


