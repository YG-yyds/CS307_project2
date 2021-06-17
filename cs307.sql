create table if not exists instructor(
    id int primary key ,
    first_name varchar,
    last_name varchar
);
create table if not exists department(
    id serial primary key ,
    name varchar unique not null
);
create table if not exists major(
    id serial primary key ,
    name varchar unique not null ,
    department_id int not null ,
    constraint fk_md foreign key (department_id) references department(id)
);
create table if not exists student(
    id int primary key ,
    first_name varchar,
    last_name varchar,
    enrolled_date date not null ,
    major_id int not null ,
    constraint fk_sm foreign key (major_id) references major(id)
);
create table if not exists semester(
    id serial primary key ,
    name varchar not null ,
    begin date unique not null ,
    "end" date unique not null
);
create table if not exists course(
    id varchar primary key ,
    name varchar not null ,
    credit int,
    class_hour int,
    course_grading varchar
);
create table if not exists course_section(
    id serial primary key ,
    name varchar not null ,
    total_capacity int,
    left_capacity int,
    course_id varchar not null ,
    semester_id int not null ,
    unique (name,course_id,semester_id),
    constraint fk_ccs foreign key (course_id) references course (id),
    constraint fk_scs foreign key (semester_id) references semester(id)
);
create table if not exists course_section_class(
    id serial primary key ,
    instructor_id int not null ,
    day_of_week varchar not null ,
    week_list varchar not null ,
    class_begin int not null ,
    class_end int not null ,
    location varchar,
    course_section_id int not null ,
    unique (instructor_id,day_of_week,week_list,class_begin,class_end,location,course_section_id),
    constraint fk_ccsi foreign key (instructor_id) references instructor(id),
    constraint fk_ccscs foreign key (course_section_id) references course_section(id)
);
create table if not exists student_course_section(
    student_id int ,
    course_section_id int ,
    grade varchar,
    primary key (student_id,course_section_id),
    constraint fk_scss foreign key (student_id) references student(id),
    constraint fk_scscs foreign key (course_section_id) references course_section(id)
);
create table if not exists prerequisite(
    course_id varchar,
    node_id serial,
    value varchar,
    children varchar,
    primary key (course_id,node_id),
    constraint fk_coi foreign key (course_id) references course(id)
);
create table if not exists major_course(
    major_id int,
    course_id varchar,
    type int,
    primary key (major_id,course_id,type),
    constraint fk_mcm foreign key (major_id) references major(id),
    constraint fk_mcc foreign key (course_id) references course(id)
);
create or replace view search_course_view
    (c_id,c_name,c_credit,c_class_hour,c_grading,
     cs_id,cs_name,cs_total_capacity,cs_left_capacity,
     i_id,i_first_name,i_last_name,
     csc_id,csc_day_of_week,csc_week_list,
     csc_class_begin,csc_class_end,csc_location,
    semester_id)
    as
    select c.id,c.name,c.credit,c.class_hour,c.course_grading,
           cs.id,cs.name,cs.total_capacity,cs.left_capacity,
           i.id,i.first_name,i.last_name,
           csc.id,csc.day_of_week,csc.week_list,
           csc.class_begin,csc.class_end,csc.location,
           cs.semester_id
    from course c join course_section cs on c.id = cs.course_id
join course_section_class csc on cs.id = csc.course_section_id
join instructor i on i.id = csc.instructor_id
order by c.id,c.name||'['||cs.name||']';