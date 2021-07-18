create table jds_database (
  -- internal id, just exists to avoid potentially long db names in every row of jds_document and also to facilitate renaming
  id int4 not null,
  name varchar(200) collate "C" not null unique,
  -- the version of the database
  version varchar(200) collate "C" not null,
  -- TODO: authorization
  primary key (id)
);

create table jds_document (
  database_id int4 not null,
  id varchar(200) collate "C" not null,
  -- the version of the document
  version varchar(200) collate "C" not null,
  data jsonb not null,
  primary key (database_id, id),
  foreign key (database_id) references jds_database (id)
);

create index idx_jds_document_data on jds_document using gin (data jsonb_path_ops);


-- TODO: jds_attachment, jds_user?
