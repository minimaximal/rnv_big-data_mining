create table linegroups
(
    linegroup_id varchar(255) not null
        primary key
);

create table `lines`
(
    id                   int(10) auto_increment
        primary key,
    api_id               varchar(255) not null,
    api_destinationLabel varchar(255) null,
    linegroup            varchar(255) null,
    constraint lines_pk
        unique (api_id, api_destinationLabel),
    constraint lines_ibfk_1
        foreign key (linegroup) references linegroups (linegroup_id)
);

create table journeys
(
    id                            int(10) auto_increment
        primary key,
    api_line                      int        not null,
    api_fistStop_plannedDeparture timestamp  not null,
    api_canceled                  tinyint(1) null,
    constraint journeys_lines_id_fk
        foreign key (api_line) references `lines` (id)
);

create index linegroup
    on `lines` (linegroup);

create table stations
(
    api_id            int(10)         not null,
    api_hafasID       int(10)         not null
        primary key,
    api_globalID      varchar(255)    null,
    api_name          varchar(255)    not null,
    api_longName      varchar(255)    not null,
    api_place         varchar(255)    null,
    api_location_lat  decimal(20, 10) not null,
    api_location_long decimal(20, 10) not null,
    api_location_hash varchar(255)    not null,
    api_hasVRNStops   tinyint(1)      null,
    constraint stations_pk
        unique (api_id)
);

create table stops
(
    id                    int(10) auto_increment
        primary key,
    api_line              int(10)   not null,
    api_station           int(10)   not null,
    api_journey           int(10)   not null,
    api_plannedDeparture  timestamp null,
    api_realtimeDeparture timestamp null,
    constraint stops_pk
        unique (api_line, api_station, api_journey),
    constraint stops_journeys_id_fk
        foreign key (api_journey) references journeys (id),
    constraint stops_lines_id_fk
        foreign key (api_line) references `lines` (id),
    constraint stops_stations_api_hafasID_fk
        foreign key (api_station) references stations (api_hafasID)
);

