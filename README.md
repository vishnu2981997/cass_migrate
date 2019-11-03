# cass_migrate

Python script to perform migrations for cql scripts

### Requirements

1. java version >= 8
2. python >=2.7 or >= 3.6
3. cassandra db 
4. cassandra-driver (pip install cassandra-driver)

### Procedure

Assuming that we have cassandra db setup in our system(**windows system**)

1. navigate to ur cassandra db config folder. In my case its 
   
   **C:\Users\USER\Downloads\apache-cassandra-3.11.4\conf**
   
   change the value of **authorizer** to **AllowAllAuthorizer**
   
   i.e **authorizer: AllowAllAuthorizer**

2. navigate to the bin folder via cmd. In my case its 
   
   **C:\Users\USER\Downloads\apache-cassandra-3.11.4\bin**
   
   then do **cassandra**

3. open another terminal in the same bin location and do **cqlsh**
   
   **for dooin so u need to have python 2.7**
   
   cassandra dbs command line interface can only be accessed if u hv py2.7

4. if uhv successfully entered cassandras commandline
   
   lets create a super user but first get out of cqlsh by hittin **exit** in cmd
   
   for creating super user do the following
 
    ```
    1. cqlsh -u cassandra -p cassandra
    
    2. CREATE ROLE was_up_bro with SUPERUSER = true AND LOGIN = true and PASSWORD = 'was_up';
       
    3. EXIT;
   ```

5. now lets login with the newly created user and 
   
   create a new keyspace(DataBase)
   
   ```
   1. cqlsh -u was_up_bro -p was_up
   
   2. CREATE KEYSPACE IF NOT EXISTS rim_jim_cutie_pie WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '3' }
   ```
   
6. now i guess if everythin went well, we r good to perform migrations

7. so, lets perform a migration
   
   if u can see we have sm set of scripts in scripts path
   
   lets use a set from those set of scripts
   
   navigate to the folder where cass_migrates manage.py is located
   
   in my case **D:\vishnu personal\git_kraken_repositories\cass_migrate**
   
   and execute the followin cmd
   
   ```
   python manage.py host user_name password port key_space application_name env_name cql_files_path mode
   ```
   
   **eg :**
   
   for creating a migration ie. up set **mode** as **up**
   
   ```
   python manage.py 127.0.0.1 was_up_bro was_up 9042 rim_jim_cutie_pie app_1 dev test_1 up
   ```
   
   for goin back to a previous version i.e down set **mode** as **down**
   
   ```
   python manage.py 127.0.0.1 was_up_bro was_up 9042 rim_jim_cutie_pie app_1 dev test_1 down
   ```
   
   down can only be performed if there is a successful up that has been performed on the specified keyspace
   
#### **if smthin is wrong check the logs to see what went wrong**

#### **if ya feel like smthis is fishy create another keyspace and use it**