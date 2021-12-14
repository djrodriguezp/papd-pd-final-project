CREATE TABLE covid19.global_data(
    id int primary key auto_increment,
    country varchar(100),
    state varchar(100),
    lat float,
    lon float,
    date date,
    cases int,
    status varchar(20)
);