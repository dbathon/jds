-- create database
create database jds;

-- create the user/role
create user jds with encrypted password 'jds';
grant all privileges on database jds to jds;
