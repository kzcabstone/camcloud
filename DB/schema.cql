
CREATE KEYSPACE camcloud WITH replication =
{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

USE camcloud;

CREATE TABLE records
(
    id                  timeuuid,
    ts                  timestamp,
    cam                 text,
    software            text,
    vehicle_color       text,
    vehicle_type        text,
    vehicle_type_score  double,
    vehicle_plate       text,
    object_label        text,
    object_score        double,
    object_id           int,
    object_x            int,
    object_y            int,
    object_w            int,
    object_h            int,
    PRIMARY KEY (cam, vehicle_color, vehicle_type, vehicle_plate, id)
);

