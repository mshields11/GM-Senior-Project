GM FinTech
README

Mohamad Saab | Abdul Ahmad | John Gettel | Aalem Singh  | CSC4996 | December 11 2019


1. Create a folder 'gmfintech' at a convenient location on machine

2. Clone 'master' branch of GitLab Repository in 'gmfintech' folder:  
	git clone https://gitlab.com/jwgettel/gmfintechf2019

3. Launch MySQL workbench

4. Creat a new connection:  
	-Connection Name: localhost  
	-Hostname: 127.0.0.1    
	-Port: 3306  
	-Username: root  
	-Password: password  
	(or edit connection string in:  
	-> gmfintech2019 -> FinsterTab -> F2019 -> dbEngine.py)

5. Create 'gmfsp_db' schema in MySQL Workbench

6. Copy SQL script from folder:  
	-> gmfintech2019 -> FinsterTab -> F2019 -> SQL -> SQL CREATE TABEL SCRIPTS MYSQL -> CREATE_ALL_DATABASE_TABLES_MYSQL.sql  
	
7.	Run script in MySQL Workbench					

8. Install PyCharm Professional Edition

9. Add Python Interpreter

10. Add all dependencies
	-xgboost  
	-sqlalchemy  
	-pandas_datareader  
	-stockstats  
	-statmodels  
	-sklearn  
	-DBEngine

11. Run DataMain.py in PyCharm

12. Open Tableau Dashboard from cloned directory
